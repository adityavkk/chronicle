//! Local durable acceptance is not committed semantic execution; see ASYNC.md.
use super::*;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use serde_json::json;

const RESULT_BATCHES: usize = 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Position {
    pub log_id: LogId,
    pub ordinal: usize,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Receipt {
    cluster: String,
    group: usize,
    position: Position,
}
impl Receipt {
    fn encode(&self) -> String {
        format!(
            "er1.{}",
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(self).unwrap())
        )
    }
    fn decode(token: &str, cluster: &str, groups: usize) -> Option<Self> {
        if token.len() > 2048 {
            return None;
        }
        let data = URL_SAFE_NO_PAD.decode(token.strip_prefix("er1.")?).ok()?;
        let receipt: Self = serde_json::from_slice(&data).ok()?;
        (receipt.cluster == cluster
            && receipt.group < groups
            && receipt.position.ordinal < batch::MAX_COMMANDS)
            .then_some(receipt)
    }
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub(super) struct State {
    batches: BTreeMap<u64, Results>,
}
#[derive(Clone, Serialize, Deserialize)]
struct Results {
    log_id: LogId,
    replies: BTreeMap<usize, Reply>,
}
impl State {
    pub fn record(&mut self, position: Position, reply: Reply) {
        let batch = self
            .batches
            .entry(position.log_id.index)
            .or_insert_with(|| Results {
                log_id: position.log_id,
                replies: BTreeMap::new(),
            });
        assert_eq!(
            batch.log_id, position.log_id,
            "committed identity cannot change"
        );
        batch.replies.insert(position.ordinal, reply);
        while self.batches.len() > RESULT_BATCHES {
            self.batches.pop_first();
        }
    }

    /// Absence (including eviction) never proves invalidation. A retained,
    /// APPLIED replacement does. Compare term AND leader as well as index.
    pub fn lookup(
        &self,
        position: Position,
        applied: Option<LogId>,
        retained: Option<LogId>,
    ) -> (&'static str, Option<&Reply>) {
        let cached = self.batches.get(&position.log_id.index);
        if let Some(batch) = cached.filter(|b| b.log_id == position.log_id) {
            if let Some(reply) = batch.replies.get(&position.ordinal) {
                return (
                    if reply.status < 400 {
                        "committed"
                    } else {
                        "rejected"
                    },
                    Some(reply),
                );
            }
        }
        let known = retained
            .or_else(|| cached.map(|b| b.log_id))
            .or_else(|| applied.filter(|id| id.index == position.log_id.index));
        if applied.is_some_and(|id| id.index >= position.log_id.index) {
            if known.is_some_and(|id| id != position.log_id) {
                return ("invalidated", None);
            }
        } else if retained == Some(position.log_id) {
            return ("pending", None);
        }
        ("unknown", None)
    }
}

impl Cluster {
    pub(super) fn accepted(&self, group: usize, position: Position) -> Resp {
        let token = Receipt {
            cluster: self.config.cluster.clone(),
            group,
            position,
        }
        .encode();
        let location = format!("/_receipts/{token}");
        let mut resp = json(&json!({"state":"accepted","receipt":token,"location":location}));
        resp.status = 202;
        resp.headers.extend([
            ("stream-durability", "local-fsync".into()),
            ("stream-receipt", token),
            ("location", location),
            ("cache-control", "no-store".into()),
        ]);
        resp
    }

    pub(super) async fn receipt(&self, req: Req) -> Resp {
        if req.method != Method::Get {
            return response(405, "receipt lookup requires GET");
        }
        let Some(receipt) = Receipt::decode(
            req.path.trim_start_matches("/_receipts/"),
            &self.config.cluster,
            self.groups.len(),
        ) else {
            return response(400, "invalid receipt");
        };
        let wait = match req.query.as_deref() {
            None | Some("") => 0,
            Some(query) => match query
                .strip_prefix("wait_ms=")
                .and_then(|s| s.parse::<u64>().ok())
            {
                Some(n) if n <= 30_000 => n,
                _ => return response(400, "expected wait_ms between 0 and 30000"),
            },
        };
        let group = &self.groups[receipt.group];
        let deadline = tokio::time::Instant::now() + Duration::from_millis(wait);
        let lookup = async {
            loop {
                let changed = group.machine.journal.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                let view = group.machine.view.read().await;
                let retained = group.machine.journal.id_at(receipt.position.log_id.index);
                let (state, reply) = view
                    .receipts
                    .lookup(receipt.position, view.applied, retained);
                if state != "pending" || tokio::time::Instant::now() >= deadline {
                    let progress = group.raft.metrics().borrow_watched().clone();
                    let session = (state == "committed")
                        .then(|| self.token(receipt.group, receipt.position.log_id.index));
                    let mut resp = json(&json!({"state":state,"response":reply,"session":session,
                        "progress":{"applied":view.applied,"last_log_index":progress.last_log_index,
                            "leader":progress.current_leader,
                            "pending_commands":self.config.pending_commands-group.slots.available_permits(),
                            "pending_bytes":self.config.pending_bytes-group.bytes.available_permits(),
                            "max_pending_commands":self.config.pending_commands,
                            "max_pending_bytes":self.config.pending_bytes,
                            "recovering":progress.current_leader != Some(progress.id)
                                || group.admitted_term.load(Ordering::Acquire) != progress.current_term}}));
                    resp.status = match state {
                        "pending" => 202,
                        "unknown" => 404,
                        "invalidated" => 410,
                        _ => 200,
                    };
                    resp.headers.push(("cache-control", "no-store".into()));
                    if let Some(session) = session {
                        resp.headers.push(("stream-session", session));
                    }
                    return resp;
                }
                drop(view);
                let _ = tokio::time::timeout_at(deadline, changed).await;
            }
        };
        match tokio::time::timeout(Duration::from_millis(wait + 3000), lookup).await {
            Ok(resp) => resp,
            Err(_) => response(503, "receipt lookup unavailable; outcome unknown"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::vote::RaftLeaderId;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn full_identity_ordinal_retention_and_semantic_result(
            term in 1u64..99, leader in 1u64..99, index in 2u64..999,
            ordinal in 0usize..64, rejected in any::<bool>(),
        ) {
            let id = |term, leader| LogId::new(CommittedLeaderId::new(term,leader),index);
            let pos = Position { log_id:id(term,leader), ordinal };
            let receipt = Receipt { cluster:"test-cluster".into(),group:1,position:pos };
            assert_eq!(Receipt::decode(&receipt.encode(),"test-cluster",2).unwrap().position,pos);
            assert!(Receipt::decode(&receipt.encode(),"other",2).is_none());
            assert!(Receipt::decode(&receipt.encode(),"test-cluster",1).is_none());
            let outside = Receipt {position:Position {ordinal:64,..pos},..receipt};
            assert!(Receipt::decode(&outside.encode(),"test-cluster",2).is_none());
            let mut state = State::default();
            let prior = Some(LogId::new(CommittedLeaderId::new(term,leader),index-1));
            assert_eq!(state.lookup(pos,prior,Some(pos.log_id)).0,"pending");
            assert_eq!(state.lookup(pos,prior,None).0,"unknown");
            for replacement in [id(term+1,leader),id(term,leader+1)] {
                assert_eq!(state.lookup(pos,prior,Some(replacement)).0,"unknown");
                assert_eq!(state.lookup(pos,Some(replacement),Some(replacement)).0,"invalidated");
                assert_eq!(state.lookup(pos,Some(replacement),None).0,"invalidated");
            }
            let reply = Reply {status:if rejected {409} else {204},body:vec![37,99],headers:vec![]};
            state.record(pos,reply);
            let (kind, reply) = state.lookup(pos,Some(pos.log_id),None);
            assert_eq!(kind,if rejected {"rejected"} else {"committed"});
            assert_eq!(reply.unwrap().body,vec![37,99]);
            assert_eq!(state.lookup(Position {ordinal:ordinal+1,..pos},Some(pos.log_id),None).0,"unknown");
            for n in index+1..=index+RESULT_BATCHES as u64 {
                state.record(Position {log_id:LogId::new(CommittedLeaderId::new(term,leader),n),ordinal:0},Reply::default());
            }
            assert_eq!(state.batches.len(),RESULT_BATCHES);
            // Eviction is not loss, even with a retained exact committed log.
            assert_eq!(state.lookup(pos,Some(pos.log_id),Some(pos.log_id)).0,"unknown");
        }
    }
}
