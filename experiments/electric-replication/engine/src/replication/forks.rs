//! Durable cross-group ownership decisions and native-wire materialization.
//! All transitions run under Machine::view after quorum commit; no network I/O.
use super::*;
use crate::handlers::{create_prepared, prepare_create, PreparedCreate};
use crate::store::{CreateResult, StreamConfig};

pub const CHUNK: usize = 64 * 1024;

#[derive(Clone, Serialize, Deserialize)]
pub struct Grant {
    pub source: String,
    pub incarnation: u64,
    pub config: StreamConfig,
    pub end: u64,
    pub time: u64,
}

#[derive(Clone, Serialize, Deserialize)]
pub enum Decision {
    Granted(Grant),
    Aborted(Reply),
    Released,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Destination {
    pub path: String,
    pub source_group: usize,
    pub source_id: u64,
    pub config: StreamConfig,
    pub headers: Vec<(String, String)>,
    pub wire: Vec<u8>,
    pub grant: Option<Grant>,
    pub created: Option<u64>,
    pub result: Option<Reply>,
    pub retired: bool,
    pub released: bool,
}

#[derive(Default, Serialize, Deserialize)]
pub struct State {
    /// Only pending names. Published native objects own their ordinary names.
    pub reservations: BTreeMap<String, String>,
    pub destinations: BTreeMap<String, Destination>,
    pub decisions: BTreeMap<String, Decision>,
    /// Mirrors are soft-deleted native parents, hidden from public catalogs.
    pub mirrors: BTreeMap<String, u64>,
}

#[derive(Clone, Serialize, Deserialize)]
pub enum Action {
    Reserve {
        group: usize,
        source_group: usize,
        source_id: u64,
        config: StreamConfig,
        headers: Vec<(String, String)>,
        wire: Vec<u8>,
    },
    Grant {
        tx: String,
        source_id: u64,
        headers: Vec<(String, String)>,
        expected: StreamConfig,
    },
    Accept {
        tx: String,
        decision: Decision,
    },
    Chunk {
        tx: String,
        start: u64,
        bytes: Vec<u8>,
    },
    Publish {
        tx: String,
    },
    Retire {
        tx: String,
    },
    Release {
        tx: String,
    },
    Released {
        tx: String,
    },
}

/// Headers that define native PUT configuration, in deterministic order. Host
/// affects Location only; initial data is intentionally ignored by a re-PUT.
pub fn identity(headers: &[(String, String)]) -> Vec<(String, String)> {
    const NAMES: &[&str] = &[
        "content-type",
        "stream-ttl",
        "stream-expires-at",
        "stream-closed",
        "stream-forked-from",
        "stream-fork-offset",
        "stream-fork-sub-offset",
    ];
    NAMES
        .iter()
        .filter_map(|name| headers.iter().find(|(k, _)| k == name).cloned())
        .collect()
}

fn missing() -> Resp {
    response(409, "fork transaction not active")
}
fn corrupt(message: &str) -> io::Error {
    io::Error::other(message)
}

impl State {
    pub fn pending(&self, path: &str) -> bool {
        self.reservations.contains_key(path)
    }

    pub async fn apply(
        &mut self,
        store: &Arc<Store>,
        path: &str,
        action: Action,
        index: u64,
    ) -> io::Result<Resp> {
        Ok(match action {
            Action::Reserve {
                group,
                source_group,
                source_id,
                config,
                headers,
                wire,
            } => {
                if let Some(tx) = self.reservations.get(path) {
                    if identity(&headers) != identity(&self.destinations[tx].headers) {
                        return Ok(response(
                            409,
                            "destination reserved with different configuration",
                        ));
                    }
                    return Ok(Resp {
                        status: 202,
                        ..json(tx)
                    });
                }
                if store.get(path).is_some() {
                    return Ok(create_prepared(
                        store.clone(),
                        path.into(),
                        PreparedCreate {
                            config,
                            parent: None,
                            base_offset: 0,
                            wire: None,
                            host: None,
                        },
                    )
                    .await);
                }
                if self.reservations.len() >= 32 {
                    return Ok(response(429, "pending fork bound reached"));
                }
                let tx = format!("{group}:{index}");
                self.reservations.insert(path.into(), tx.clone());
                self.destinations.insert(
                    tx.clone(),
                    Destination {
                        path: path.into(),
                        source_group,
                        source_id,
                        config,
                        headers,
                        wire,
                        grant: None,
                        created: None,
                        result: None,
                        retired: false,
                        released: false,
                    },
                );
                Resp {
                    status: 202,
                    ..json(&tx)
                }
            }
            Action::Grant {
                tx,
                source_id,
                headers,
                expected,
            } => {
                if let Some(decision) = self.decisions.get(&tx) {
                    return Ok(json(decision));
                }
                if self.pending(path) {
                    return Ok(response(409, "source fork is still materializing"));
                }
                let request = Req {
                    method: Method::Put,
                    path: String::new(),
                    query: None,
                    headers,
                    body: Default::default(),
                };
                let decision = match prepare_create(store, &request).await {
                    Ok(prepared) => {
                        let source = prepared
                            .parent
                            .ok_or_else(|| corrupt("grant without source"))?;
                        if source.path != path
                            || source.id != source_id
                            || prepared.config != expected
                        {
                            Decision::Aborted(Reply::from_resp(response(
                                409,
                                "source incarnation/configuration changed",
                            )))
                        } else {
                            store.retain_reference(&source)?;
                            Decision::Granted(Grant {
                                source: path.into(),
                                incarnation: source_id,
                                config: prepared.config,
                                end: prepared.base_offset,
                                time: clock::millis(store.clock.now()),
                            })
                        }
                    }
                    Err(resp) if resp.status < 500 => Decision::Aborted(Reply::from_resp(resp)),
                    Err(_) => return Err(corrupt("source grant range read failed")),
                };
                self.decisions.insert(tx, decision.clone());
                json(&decision)
            }
            Action::Accept { tx, decision } => {
                let Some(dest) = self.destinations.get_mut(&tx) else {
                    return Ok(missing());
                };
                if dest.grant.is_some() || dest.result.is_some() {
                    return Ok(Resp::new(204));
                }
                match decision {
                    Decision::Aborted(reply) => {
                        self.reservations.remove(&dest.path);
                        dest.result = Some(reply);
                        dest.wire.clear();
                        dest.released = true;
                    }
                    Decision::Granted(grant) => {
                        if grant.incarnation != dest.source_id || grant.config != dest.config {
                            return Err(corrupt("grant does not match reservation"));
                        }
                        let mirror = if let Some(stream) = store.streams.get(&grant.source) {
                            if self.mirrors.get(&grant.source) != Some(&grant.incarnation) {
                                return Err(corrupt("live mirror incarnation collision"));
                            }
                            stream.clone()
                        } else {
                            let config = StreamConfig {
                                content_type: grant.config.content_type.clone(),
                                ttl_seconds: None,
                                expires_at: None,
                                expires_at_raw: None,
                                create_closed: false,
                                forked_from: None,
                                fork_offset_raw: None,
                                fork_sub_offset: None,
                            };
                            let CreateResult::Created(stream) =
                                store.create(&grant.source, config, None, 0)?
                            else {
                                return Err(corrupt("mirror create conflict"));
                            };
                            stream.shared.write().unwrap().soft_deleted = true;
                            self.mirrors.insert(grant.source.clone(), grant.incarnation);
                            stream
                        };
                        // Keep the mirror through import even before a native child exists.
                        store.retain_reference(&mirror)?;
                        dest.grant = Some(grant);
                    }
                    Decision::Released => {
                        return Err(corrupt("grant released before destination publication"))
                    }
                }
                Resp::new(204)
            }
            Action::Chunk { tx, start, bytes } => {
                let Some(dest) = self.destinations.get(&tx) else {
                    return Ok(missing());
                };
                if dest.result.is_some() {
                    return Ok(Resp::new(204));
                }
                let Some(grant) = &dest.grant else {
                    return Ok(missing());
                };
                let mirror = store
                    .streams
                    .get(&grant.source)
                    .ok_or_else(|| corrupt("missing pinned mirror"))?
                    .clone();
                let end = start
                    .checked_add(bytes.len() as u64)
                    .ok_or_else(|| corrupt("chunk offset overflow"))?;
                if bytes.is_empty() || bytes.len() > CHUNK || end > grant.end {
                    return Ok(response(400, "invalid import chunk"));
                }
                let tail = mirror.tail().bytes;
                if end <= tail {
                    return Ok(Resp::new(204));
                } // immutable prefix, duplicate transfer
                if start != tail {
                    return Ok(response(409, "import chunk not contiguous"));
                }
                let wire = bytes.into();
                let mut appender = mirror.appender.lock().await;
                let tail = crate::handlers::write_wire(&mirror, &mut appender, &wire)?;
                crate::handlers::publish_durable_tail(&mirror, tail, &wire);
                Resp::new(204)
            }
            Action::Publish { tx } => {
                let Some(dest) = self.destinations.get_mut(&tx) else {
                    return Ok(missing());
                };
                if let Some(reply) = &dest.result {
                    return Ok(reply.clone().into_resp());
                }
                let Some(grant) = &dest.grant else {
                    return Ok(missing());
                };
                let mirror = store
                    .streams
                    .get(&grant.source)
                    .ok_or_else(|| corrupt("missing pinned mirror"))?
                    .clone();
                if mirror.tail().bytes < grant.end {
                    return Ok(response(409, "fork prefix not fully imported"));
                }
                let reply = create_prepared(
                    store.clone(),
                    dest.path.clone(),
                    PreparedCreate {
                        config: grant.config.clone(),
                        parent: Some(mirror.clone()),
                        base_offset: grant.end,
                        wire: Some(dest.wire.clone().into()),
                        host: dest
                            .headers
                            .iter()
                            .find(|(k, _)| k == "host")
                            .map(|(_, v)| v.clone()),
                    },
                )
                .await;
                if reply.status != 201 {
                    return Err(corrupt("reserved native child publication failed"));
                }
                let child = store
                    .streams
                    .get(&dest.path)
                    .ok_or_else(|| corrupt("published child missing"))?
                    .clone();
                child.shared.write().unwrap().last_access =
                    std::time::UNIX_EPOCH + Duration::from_millis(grant.time);
                crate::store::write_meta_sync(&child, true)?;
                store.release_reference(&mirror)?; // native child now owns this reference
                dest.created = Some(child.id);
                dest.result = Some(Reply::from_resp(reply));
                dest.wire.clear();
                self.reservations.remove(&dest.path);
                dest.result.as_ref().unwrap().clone().into_resp()
            }
            Action::Retire { tx } => {
                let Some(dest) = self.destinations.get_mut(&tx) else {
                    return Ok(missing());
                };
                let Some(created) = dest.created else {
                    return Ok(missing());
                };
                // Raw map, not public lookup: soft deletion with grandchildren
                // must retain the remote grant even though GET returns 410.
                if store
                    .streams
                    .get(&dest.path)
                    .is_some_and(|s| s.id == created)
                {
                    return Ok(response(409, "native child is still retained"));
                }
                dest.retired = true;
                Resp::new(204)
            }
            Action::Release { tx } => {
                match self.decisions.get(&tx) {
                    Some(Decision::Granted(grant)) => {
                        let source = store
                            .streams
                            .get(&grant.source)
                            .ok_or_else(|| corrupt("retained source missing"))?
                            .clone();
                        if source.id != grant.incarnation {
                            return Err(corrupt("retained source recreated"));
                        }
                        store.release_reference(&source)?;
                    }
                    Some(Decision::Released) => return Ok(Resp::new(204)),
                    _ => return Ok(missing()),
                }
                self.decisions.insert(tx, Decision::Released);
                Resp::new(204)
            }
            Action::Released { tx } => {
                let Some(dest) = self.destinations.get_mut(&tx) else {
                    return Ok(missing());
                };
                if !dest.retired {
                    return Ok(missing());
                }
                dest.released = true;
                Resp::new(204)
            }
        })
    }
}
