//! Strict SSE observations; watch notifications never authorize a cursor.
use std::{io, sync::Arc, time::Duration};

use axum::{
    body::{Body, BodyDataStream},
    response::Response,
};
use bytes::Bytes;
use chronicle_raft::{
    model,
    sse_wire::{self, Encoding},
    storage::{ReadError, StreamInfo},
    wire,
};
use futures_util::{StreamExt, stream};
use tokio::{
    sync::{OwnedSemaphorePermit, watch},
    time::{Instant, timeout_at},
};

use crate::{ApiResult, Shared, bad, read_visible_info, telemetry::PhaseTimings, unavailable};

const LIFETIME: Duration = Duration::from_secs(60);
const HEARTBEAT: Duration = Duration::from_secs(15);

struct Reader {
    app: Shared,
    shard: u64,
    key: String,
    offset: u64,
    view: StreamInfo,
    encoding: Encoding,
    changes: watch::Receiver<()>,
    client_cursor: Option<u64>,
    admission: [Arc<OwnedSemaphorePermit>; 2],
    data: Option<BodyDataStream>,
    reported: bool,
    deadline: Instant,
    heartbeat: Instant,
}

pub async fn response(
    app: Shared,
    key: String,
    offset: u64,
    view: StreamInfo,
    changes: watch::Receiver<()>,
    client_cursor: Option<u64>,
    admission: [Arc<OwnedSemaphorePermit>; 2],
) -> ApiResult {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    wire::compute_cursor(client_cursor, now).map_err(bad)?;
    let encoding = if view.config.content_type.starts_with("application/json") {
        Encoding::Json
    } else if view.config.content_type.starts_with("text/") {
        Encoding::Text
    } else {
        Encoding::Base64
    };
    let mut response = Response::builder()
        .header("content-type", "text/event-stream")
        .header("cache-control", "no-store")
        .header("x-content-type-options", "nosniff")
        .header("stream-consistency", "strict")
        .header("stream-incarnation", view.incarnation.to_string());
    if encoding == Encoding::Base64 {
        response = response.header("stream-sse-data-encoding", "base64");
    }
    let mut reader = Reader {
        shard: model::shard(&key),
        app,
        key,
        offset,
        view,
        encoding,
        changes,
        client_cursor,
        admission,
        data: None,
        reported: false,
        deadline: Instant::now() + LIFETIME,
        heartbeat: Instant::now(),
    };
    // Validate JSON boundaries and capture the initial inode before committing HTTP headers.
    reader.open_data().await.map_err(|e| match e {
        ReadError::Offset => bad(e),
        _ => unavailable(e),
    })?;
    response
        .body(Body::from_stream(stream::try_unfold(
            reader,
            |mut reader| async move {
                Ok::<_, io::Error>(reader.next().await?.map(|bytes| (bytes, reader)))
            },
        )))
        .map_err(unavailable)
}

impl Reader {
    async fn open_data(&mut self) -> Result<(), ReadError> {
        if self.view.end == self.offset {
            return Ok(());
        }
        let file = timeout_at(
            self.deadline,
            self.app.groups[&self.shard].store.read_file(
                self.key.clone(),
                &self.view,
                self.offset,
                self.admission.clone(),
            ),
        )
        .await
        .map_err(|error| ReadError::Io(io::Error::new(io::ErrorKind::TimedOut, error)))??;
        let json = self.encoding == Encoding::Json;
        let length = (self.view.end - self.offset).saturating_sub(u64::from(json));
        self.data = Some(
            sse_wire::data_body(
                wire::file_body(file, length, json, self.admission.clone()),
                self.encoding,
            )
            .into_data_stream(),
        );
        Ok(())
    }

    async fn next(&mut self) -> io::Result<Option<Bytes>> {
        loop {
            if Instant::now() >= self.deadline {
                return if self.data.is_some() || !self.reported {
                    Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "SSE lifetime ended during delivery",
                    ))
                } else {
                    Ok(None)
                };
            }
            if let Some(data) = &mut self.data {
                match timeout_at(self.deadline, data.next())
                    .await
                    .map_err(io::Error::other)?
                {
                    Some(bytes) => return bytes.map(Some).map_err(io::Error::other),
                    None => self.data = None,
                }
            }
            if !self.reported {
                // A control is emitted only after the entire data event succeeded.
                self.offset = self.view.end;
                self.reported = true;
                self.heartbeat = Instant::now() + HEARTBEAT;
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default();
                let cursor =
                    wire::compute_cursor(self.client_cursor, now).map_err(io::Error::other)?;
                return Ok(Some(sse_wire::control(
                    self.offset,
                    cursor,
                    self.view.closed,
                )));
            }
            if self.view.closed || Instant::now() >= self.deadline {
                return Ok(None);
            }
            tokio::select! {
                result = self.changes.changed() => { result.map_err(io::Error::other)?; }
                _ = tokio::time::sleep_until(self.heartbeat.min(self.deadline)) => {}
            }
            if Instant::now() >= self.deadline {
                return Ok(None);
            }
            let view = timeout_at(
                self.deadline,
                read_visible_info(
                    &self.app.groups[&self.shard],
                    &self.key,
                    false,
                    &mut PhaseTimings::default(),
                ),
            )
            .await
            .map_err(io::Error::other)?
            .map_err(|(_, error)| io::Error::other(error))?
            .ok_or_else(|| io::Error::other("SSE stream disappeared"))?;
            if view.incarnation != self.view.incarnation || view.end < self.offset {
                return Err(io::Error::other("SSE stream incarnation/frontier changed"));
            }
            let changed = view.end > self.offset || view.closed;
            self.view = view;
            if changed || Instant::now() >= self.heartbeat {
                self.open_data().await.map_err(io::Error::other)?;
                self.reported = false;
            }
        }
    }
}
