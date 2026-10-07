//! Retriable transaction-fenced cross-group calls. No network I/O under apply.
use super::*;
use forks::{Action, Decision, CHUNK};

#[derive(Serialize, Deserialize)]
pub enum Call {
    Probe { headers: Vec<(String, String)> },
    Apply { path: String, action: Action },
    Range { tx: String, start: u64 },
}

impl Cluster {
    async fn fork_propose(&self, group: usize, path: &str, action: Action) -> Result<Reply, u16> {
        let body = bincode::serialize(&action).unwrap();
        if body.len() > 1024 * 1024 {
            return Err(413);
        }
        self.groups[group]
            .propose(Command {
                method: "FORK".into(),
                path: path.into(),
                headers: vec![],
                body,
                time: clock::millis(std::time::SystemTime::now()),
            })
            .await
            .map(|r| r.data)
    }

    async fn fork_remote(&self, group: usize, call: Call) -> Result<Reply, u16> {
        serde_json::from_value(self.control_read(group, "fork", &call).await?).map_err(|_| 503)
    }

    pub async fn fork_control(&self, group: usize, req: Req) -> Resp {
        let Ok(call) = serde_json::from_slice::<Call>(&req.body) else {
            return response(400, "invalid fork call");
        };
        let reply = match call {
            Call::Apply { path, action } => match action {
                // These two operations have durable transaction-ID deduplication.
                Action::Grant { .. } | Action::Release { .. } => {
                    match self.fork_propose(group, &path, action).await {
                        Ok(reply) => reply,
                        Err(status) => return response(status, "fork decision outcome unknown"),
                    }
                }
                _ => return response(400, "not a source-owner operation"),
            },
            Call::Probe { headers } => {
                let source = headers
                    .iter()
                    .find(|(k, _)| k == "stream-forked-from")
                    .map(|(_, v)| v.clone())
                    .unwrap_or_default();
                if partition(&source, self.groups.len()) != group
                    || subscription_io::reserved(&source)
                {
                    return json(&Reply::from_resp(response(400, "invalid fork source path")));
                }
                // A source with TTL must resolve against a committed clock, not
                // the receiving worker's wall clock. This also confirms leader.
                if self.groups[group]
                    .propose(Command {
                        method: "TICK".into(),
                        path: source.clone(),
                        headers: vec![],
                        body: vec![],
                        time: clock::millis(std::time::SystemTime::now()),
                    })
                    .await
                    .is_err()
                {
                    return response(503, "source validation barrier unavailable");
                }
                let view = self.groups[group].machine.view.read().await;
                if view.forks.pending(&source) {
                    return response(503, "source is still materializing");
                }
                let request = Req {
                    method: Method::Put,
                    path: String::new(),
                    query: None,
                    headers,
                    body: Default::default(),
                };
                Reply::from_resp(
                    match crate::handlers::prepare_create(&view.store, &request).await {
                        Ok(prepared) => json(&(prepared.parent.unwrap().id, prepared.config)),
                        Err(resp) => resp,
                    },
                )
            }
            Call::Range { tx, start } => {
                if self.barrier(group).await.is_err() {
                    return response(503, "range barrier unavailable");
                }
                let view = self.groups[group].machine.view.read().await;
                let Some(Decision::Granted(grant)) = view.forks.decisions.get(&tx) else {
                    return json(&Reply::from_resp(response(409, "no retained fork grant")));
                };
                if start >= grant.end {
                    return json(&Reply::from_resp(response(400, "invalid fork range")));
                }
                let Some(source) = view.store.streams.get(&grant.source).map(|s| s.clone()) else {
                    return response(503, "retained source missing");
                };
                if source.id != grant.incarnation {
                    return response(503, "retained source incarnation mismatch");
                }
                let end = grant.end.min(start.saturating_add(CHUNK as u64));
                match crate::handlers::read_range_bytes(&source, start, end).await {
                    Ok(bytes) => Reply::from_resp(Resp {
                        status: 200,
                        headers: vec![],
                        body: Body::Full(bytes),
                    }),
                    Err(_) => return response(503, "source range read failed"),
                }
            }
        };
        json(&reply)
    }

    pub async fn remote_fork(&self, group: usize, req: Req) -> Resp {
        let pending = {
            let view = self.groups[group].machine.view.read().await;
            view.forks
                .reservations
                .get(&req.path)
                .map(|tx| (tx.clone(), view.forks.destinations[tx].headers.clone()))
        };
        let tx = if let Some((tx, headers)) = pending {
            if forks::identity(&req.headers) != forks::identity(&headers) {
                return response(409, "destination reserved with different configuration");
            }
            tx
        } else {
            let source_group =
                partition(req.header("stream-forked-from").unwrap(), self.groups.len());
            let probe = match self
                .fork_remote(
                    source_group,
                    Call::Probe {
                        headers: req.headers.clone(),
                    },
                )
                .await
            {
                Ok(reply) if reply.status == 200 => reply,
                Ok(reply) => return reply.into_resp(),
                Err(status) => {
                    return response(status, "source owner unavailable; fork not reserved")
                }
            };
            let (source_id, config): (u64, crate::store::StreamConfig) =
                match serde_json::from_slice(&probe.body) {
                    Ok(info) => info,
                    Err(_) => return response(503, "invalid source descriptor"),
                };
            let wire = if req.body.is_empty() {
                vec![]
            } else {
                match crate::handlers::encode_wire(
                    &req.body,
                    crate::store::is_json_content_type(&config.content_type),
                    true,
                ) {
                    Ok(wire) => wire.to_vec(),
                    Err(message) => return response(400, message),
                }
            };
            match self
                .fork_propose(
                    group,
                    &req.path,
                    Action::Reserve {
                        group,
                        source_group,
                        source_id,
                        config,
                        headers: req.headers,
                        wire,
                    },
                )
                .await
            {
                Ok(reply) if reply.status == 202 => {
                    serde_json::from_slice::<String>(&reply.body).unwrap()
                }
                Ok(reply) => return self.fork_response(group, reply).await,
                Err(status) => return response(status, "fork reservation outcome unknown"),
            }
        };
        // The durable worker continues after the request deadline. It does not
        // cancel a grant or infer abort from a failed connection.
        let finish = async {
            loop {
                if self.groups[group].raft.metrics().borrow().current_leader
                    != Some(self.config.node)
                {
                    return None;
                }
                {
                    let view = self.groups[group].machine.view.read().await;
                    if let Some(reply) = &view.forks.destinations[&tx].result {
                        return Some(reply.clone());
                    }
                }
                if self.fork_step(group, &tx).await.is_err() {
                    return None;
                }
            }
        };
        match tokio::time::timeout(Duration::from_secs(3), finish).await {
            Ok(Some(reply)) => self.fork_response(group, reply).await,
            _ => self.unavailable(
                group,
                "fork outcome unknown; durable materialization continues",
            ),
        }
    }

    async fn fork_response(&self, group: usize, reply: Reply) -> Resp {
        let mut resp = reply.into_resp();
        let view = self.groups[group].machine.view.read().await;
        resp.headers.push((
            "stream-session",
            self.token(group, view.applied.map_or(0, |id| id.index)),
        ));
        resp.headers
            .push(("stream-durability", "quorum-fsync".into()));
        resp
    }

    async fn fork_step(&self, group: usize, tx: &str) -> Result<(), u16> {
        let dest = {
            let view = self.groups[group].machine.view.read().await;
            let Some(dest) = view.forks.destinations.get(tx) else {
                return Ok(());
            };
            if dest.released {
                return Ok(());
            }
            dest.clone()
        };
        let source = dest.config.forked_from.as_ref().unwrap();
        let action = if dest.retired {
            let reply = self
                .fork_remote(
                    dest.source_group,
                    Call::Apply {
                        path: source.clone(),
                        action: Action::Release { tx: tx.into() },
                    },
                )
                .await?;
            if reply.status != 204 {
                return Err(reply.status);
            }
            Action::Released { tx: tx.into() }
        } else if let Some(created) = dest.created {
            let view = self.groups[group].machine.view.read().await;
            if view
                .store
                .streams
                .get(&dest.path)
                .is_some_and(|s| s.id == created)
            {
                return Ok(());
            }
            Action::Retire { tx: tx.into() }
        } else if let Some(grant) = &dest.grant {
            if self.faults.read().unwrap().pause_fork_import {
                return Err(503);
            }
            let start = {
                let view = self.groups[group].machine.view.read().await;
                // Another worker may have published and collected this mirror.
                if view.forks.destinations[tx].result.is_some() {
                    return Ok(());
                }
                let tail = view.store.streams.get(source).ok_or(503u16)?.tail().bytes;
                tail
            };
            if start >= grant.end {
                Action::Publish { tx: tx.into() }
            } else {
                let reply = self
                    .fork_remote(
                        dest.source_group,
                        Call::Range {
                            tx: tx.into(),
                            start,
                        },
                    )
                    .await?;
                if reply.status != 200 {
                    return Err(reply.status);
                }
                let expected = (grant.end - start).min(CHUNK as u64) as usize;
                if reply.body.len() != expected {
                    return Err(503);
                }
                Action::Chunk {
                    tx: tx.into(),
                    start,
                    bytes: reply.body,
                }
            }
        } else {
            let reply = self
                .fork_remote(
                    dest.source_group,
                    Call::Apply {
                        path: source.clone(),
                        action: Action::Grant {
                            tx: tx.into(),
                            source_id: dest.source_id,
                            headers: dest.headers.clone(),
                            expected: dest.config.clone(),
                        },
                    },
                )
                .await?;
            if reply.status != 200 {
                return Err(reply.status);
            }
            let decision = serde_json::from_slice(&reply.body).map_err(|_| 503u16)?;
            Action::Accept {
                tx: tx.into(),
                decision,
            }
        };
        let reply = self.fork_propose(group, &dest.path, action).await?;
        if !(200..300).contains(&reply.status) {
            return Err(reply.status);
        }
        Ok(())
    }

    pub async fn fork_worker(&'static self, group: usize) {
        let mut cursor = String::new();
        loop {
            tokio::time::sleep(Duration::from_millis(150)).await;
            if self.groups[group].raft.metrics().borrow().current_leader != Some(self.config.node) {
                continue;
            }
            let batch: Vec<_> = {
                let view = self.groups[group].machine.view.read().await;
                view.forks
                    .destinations
                    .range((
                        std::ops::Bound::Excluded(cursor.clone()),
                        std::ops::Bound::Unbounded,
                    ))
                    .take(32)
                    .map(|(tx, _)| tx.clone())
                    .collect()
            };
            if batch.is_empty() {
                cursor.clear();
                continue;
            }
            cursor = batch.last().unwrap().clone();
            for tx in batch {
                let _ = self.fork_step(group, &tx).await;
            }
        }
    }
}
