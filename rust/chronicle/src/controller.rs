//! Reconcile replicated whole-shard intents. One move at a time; retries are idempotent.
use crate::{Shared, now_ms};
use chronicle_raft::model::{Command, Placement, ReplicaHistory, SHARDS, State};
use openraft::BasicNode;
use openraft::storage::RaftStateMachine;
use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, Instant},
};

const CAMPAIGN_DELAY: Duration = Duration::from_secs(30);

#[derive(Default)]
struct Retirement {
    cursors: BTreeMap<u64, usize>,
    next_shard: u64,
}

struct Campaign {
    // Placement generation, term, current leader. Any change restarts observation.
    view: (u64, u64, u64),
    since: Instant,
}

impl Campaign {
    fn due(&mut self, view: (u64, u64, u64), now: Instant) -> bool {
        if self.view != view {
            self.view = view;
            self.since = now;
            return false;
        }
        if now.duration_since(self.since) < CAMPAIGN_DELAY {
            return false;
        }
        // Before the await: cancellation or an unknown result must not undo this.
        self.since = now;
        true
    }
}

pub async fn run(a: Shared) {
    // Native campaigns are disruptive experiments, never default balancing.
    let campaigns_enabled =
        std::env::var("CHRONICLE_EXPERIMENTAL_CAMPAIGNS").is_ok_and(|value| value == "1");
    let mut campaigns = BTreeMap::new();
    let mut retirement = Retirement::default();
    loop {
        match tokio::time::timeout(
            Duration::from_secs(20),
            tick(&a, &mut campaigns, campaigns_enabled, &mut retirement),
        )
        .await
        {
            Ok(Ok(())) => (),
            result => {
                campaigns.clear();
                tracing::warn!(error = ?result, "controller retry; outcome may be unknown");
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

async fn control(a: &Shared) -> anyhow::Result<State> {
    let g = &a.groups[&0];
    if g.raft.metrics().borrow().current_leader == Some(a.id) {
        g.raft.ensure_linearizable().await?;
        return Ok(g.store.read_state().await?);
    }
    // Seeds can all retire. Persisted registry entries are routing hints, never
    // authority: the destination still performs a strict read barrier.
    let mut nodes = a.nodes.clone();
    nodes.extend(g.store.read_state().await?.nodes);
    let hint = g.raft.metrics().borrow().current_leader;
    let mut candidates: Vec<_> = nodes.into_iter().filter(|(id, _)| *id != a.id).collect();
    candidates.sort_by_key(|(id, n)| (Some(*id) != hint, n.draining));
    for (_, n) in candidates {
        if let Ok(r) = a
            .client
            .get(format!("http://{}/admin/control", n.addr))
            .timeout(Duration::from_millis(500))
            .send()
            .await
            && r.status().is_success()
        {
            return Ok(r.json().await?);
        }
    }
    anyhow::bail!("control quorum unavailable")
}

async fn tick(
    a: &Shared,
    campaigns: &mut BTreeMap<u64, Campaign>,
    campaigns_enabled: bool,
    retirement: &mut Retirement,
) -> anyhow::Result<()> {
    let mut state = control(a).await?;
    let control_group = &a.groups[&0];
    if control_group.raft.metrics().borrow().current_leader == Some(a.id) {
        for (id, n) in &a.nodes {
            if !state.nodes.contains_key(id) {
                let result = control_group
                    .raft
                    .client_write(Command::Register {
                        id: *id,
                        node: n.clone(),
                    })
                    .await?;
                anyhow::ensure!(result.data.error.is_none(), "seed registration rejected");
            }
        }
        state = control_group.store.read_state().await?;
        if state.placements.values().all(|p| p.complete) {
            // Health is only a placement preference; it never authorizes lowering quorum.
            let mut healthy = BTreeMap::new();
            for (id, node) in state.nodes.iter().filter(|(_, n)| !n.draining) {
                if a.client
                    .get(format!("http://{}/healthz", node.addr))
                    .timeout(Duration::from_millis(500))
                    .send()
                    .await
                    .is_ok_and(|r| r.status().is_success())
                {
                    healthy.insert(*id, node.clone());
                }
            }
            if healthy.len() >= 3 {
                for shard in 0..=SHARDS {
                    let voters = target(shard, &healthy);
                    let old = state.placements.get(&shard);
                    if old.is_none_or(|p| {
                        p.voters != voters && now_ms().saturating_sub(p.changed_ms) >= 15_000
                    }) {
                        control_group
                            .raft
                            .client_write(Command::Place {
                                shard,
                                expected_generation: old.map_or(0, |p| p.generation),
                                voters,
                                now_ms: now_ms(),
                                eligible_only: true,
                            })
                            .await?;
                        break;
                    }
                }
            }
        }
    }
    state = control(a).await?;
    if campaigns_enabled {
        balance_leaders(a.id, &a.groups, &state, campaigns).await?;
    }
    for (shard, p) in &state.placements {
        let group = &a.groups[shard];
        let initial = group.raft.metrics().borrow().clone();
        if initial.current_leader != Some(a.id) {
            continue;
        }
        let matches = membership_applied(&group.store, &p.voters).await?;
        if p.retirement_known() && matches {
            continue;
        }
        let _guard = group.movement.lock().await;
        let boundary = group.raft.ensure_linearizable().await?;
        for id in &p.voters {
            if *id == a.id {
                continue;
            }
            let node = &state.nodes[id];
            if !matches {
                // Repeat even for registered learners: presence does not prove catch-up.
                group
                    .raft
                    .add_learner(*id, BasicNode::new(node.addr.clone()), true)
                    .await?;
                loop {
                    let m = group.raft.metrics().borrow().clone();
                    anyhow::ensure!(
                        m.vote == initial.vote && m.current_leader == Some(a.id),
                        "leadership changed during catch-up"
                    );
                    if m.replication
                        .as_ref()
                        .and_then(|r| r.get(id))
                        .is_some_and(|matched| *matched >= boundary)
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        }
        let latest = control(a).await?;
        anyhow::ensure!(
            latest
                .placements
                .get(shard)
                .is_some_and(|x| x.generation == p.generation),
            "placement changed"
        );
        anyhow::ensure!(
            group.raft.metrics().borrow().vote == initial.vote,
            "leadership changed"
        );
        if !matches {
            group.raft.change_membership(p.voters.clone(), true).await?;
        }
        let (_, applied_membership) = group.store.clone().applied_state().await?;
        anyhow::ensure!(
            applied_membership.membership().get_joint_config().len() == 1
                && applied_membership
                    .membership()
                    .voter_ids()
                    .collect::<BTreeSet<_>>()
                    == p.voters,
            "target membership not applied"
        );
        let membership = *applied_membership.log_id();
        anyhow::ensure!(membership.is_some(), "missing applied membership boundary");
        // Membership commitment is authoritative even if this completion record is lost.
        // The next leader can inspect/repeat the same intent safely.
        let command = Command::Placed {
            shard: *shard,
            generation: p.generation,
            membership,
        };
        if control_group.raft.metrics().borrow().current_leader == Some(a.id) {
            let result = control_group.raft.client_write(command).await?;
            anyhow::ensure!(result.data.error.is_none(), "placement completion rejected");
        } else {
            // A data leader may no longer replicate the control group. Use the
            // same bounded discovery as ingress rather than its stale leader hint.
            let mut headers = axum::http::HeaderMap::new();
            headers.insert(
                axum::http::header::CONTENT_TYPE,
                axum::http::HeaderValue::from_static("application/json"),
            );
            let response = crate::proxy(
                a,
                0,
                axum::http::Method::POST,
                "/admin/placed",
                headers,
                serde_json::to_vec(&(*shard, p.generation, membership))?.into(),
            )
            .await
            .map_err(|(status, message)| anyhow::anyhow!("{status}: {message}"))?;
            anyhow::ensure!(
                response.status().is_success(),
                "placement completion rejected"
            );
        }
        return Ok(());
    }
    // Repair always takes priority. Cleanup makes at most one peer probe per
    // local tick and rotates groups even after a timeout or unknown outcome.
    if state.placements.len() == (SHARDS + 1) as usize
        && state.placements.values().all(Placement::retirement_known)
    {
        for _ in 0..=SHARDS {
            let shard = retirement.next_shard;
            retirement.next_shard = (shard + 1) % (SHARDS + 1);
            if a.groups[&shard].raft.metrics().borrow().current_leader == Some(a.id) {
                retire_replica(
                    a,
                    shard,
                    &state,
                    retirement.cursors.entry(shard).or_default(),
                )
                .await?;
                break;
            }
        }
    }
    Ok(())
}

async fn retire_replica(
    a: &Shared,
    shard: u64,
    state: &State,
    cursor: &mut usize,
) -> anyhow::Result<bool> {
    let p = &state.placements[&shard];
    let group = &a.groups[&shard];
    let initial = group.raft.metrics().borrow().clone();
    let membership = initial.membership_config.membership();
    if membership.get_joint_config().len() != 1
        || membership.voter_ids().collect::<BTreeSet<_>>() != p.voters
    {
        return Ok(false);
    }
    // One fair, deduplicated ring. A reachable but stuck learner cannot starve
    // another identity, and cancellation cannot undo the cursor advancement.
    let mut candidates: BTreeMap<_, _> = membership
        .nodes()
        .filter(|(id, _)| !p.voters.contains(id) && **id != a.id)
        .map(|(id, node)| (*id, node.addr.clone()))
        .collect();
    candidates.extend(
        state
            .nodes
            .iter()
            .filter(|(id, _)| !p.voters.contains(id) && **id != a.id)
            .map(|(id, node)| (*id, node.addr.clone())),
    );
    let Some((id, address)) = next_candidate(&candidates, cursor) else {
        return Ok(false);
    };
    let retained = membership.get_node(&id).is_some();
    let peer = peer_metrics(a, &address, id)
        .await
        .and_then(|mut metrics| metrics.remove(&shard));
    let verified = peer.as_ref().is_some_and(|m| replica_retired(p, m));
    if retained {
        if peer.as_ref().is_some_and(|m| m.running_state.is_ok()) && !verified {
            return Ok(false); // Replication continues; never wait here for catch-up.
        }
    } else if verified || peer.as_ref().is_none_or(|m| m.running_state.is_err()) {
        return Ok(false);
    }
    let _guard = group.movement.lock().await;
    let latest = control(a).await?;
    anyhow::ensure!(
        latest
            .placements
            .get(&shard)
            .is_some_and(|x| x.complete && x.generation == p.generation)
            && group.raft.metrics().borrow().vote == initial.vote
            && group.raft.metrics().borrow().current_leader == Some(a.id),
        "retirement intent or leadership changed"
    );
    if retained {
        // Removing an unreachable learner frees replication resources but is NOT
        // evidence of local retirement. The registry preserves future discovery.
        group
            .raft
            .change_membership(
                openraft::ChangeMembers::RemoveNodes(BTreeSet::from([id])),
                false,
            )
            .await?;
        tracing::info!(
            shard,
            id,
            generation = p.generation,
            verified,
            "replica.retirement"
        );
    } else {
        group
            .raft
            .add_learner(id, BasicNode::new(address), false)
            .await?;
        tracing::info!(
            shard,
            id,
            generation = p.generation,
            "replica.demotion_retry"
        );
    }
    Ok(true)
}

fn next_candidate(candidates: &BTreeMap<u64, String>, cursor: &mut usize) -> Option<(u64, String)> {
    if candidates.is_empty() {
        return None;
    }
    let candidate = candidates.iter().nth(*cursor % candidates.len());
    *cursor = (*cursor + 1) % candidates.len();
    candidate.map(|(id, address)| (*id, address.clone()))
}

fn replica_retired(p: &Placement, metrics: &openraft::RaftMetrics<u64, BasicNode>) -> bool {
    let Some(replicas) = &p.replicas else {
        return false;
    };
    if !p.retirement_known() || p.voters.contains(&metrics.id) {
        return false;
    }
    match replicas.get(&metrics.id) {
        Some(ReplicaHistory::NonvoterAfter(boundary)) => nonvoter_applied(metrics, *boundary),
        Some(ReplicaHistory::MayVote) => false,
        // These metrics come only from the recovery-fenced endpoint below.
        None => {
            metrics.running_state.is_ok()
                && metrics.vote == openraft::Vote::default()
                && metrics.last_log_index.is_none()
                && metrics.last_applied.is_none()
                && metrics.membership_config.log_id().is_none()
                && metrics
                    .membership_config
                    .membership()
                    .voter_ids()
                    .next()
                    .is_none()
                && metrics.snapshot.is_none()
                && metrics.purged.is_none()
        }
    }
}

fn nonvoter_applied(
    metrics: &openraft::RaftMetrics<u64, BasicNode>,
    boundary: openraft::LogId<u64>,
) -> bool {
    let membership = &metrics.membership_config;
    metrics.running_state.is_ok()
        && metrics.state == openraft::ServerState::Learner
        && membership.membership().get_joint_config().len() == 1
        && !membership
            .membership()
            .voter_ids()
            .any(|id| id == metrics.id)
        && *membership.log_id() >= Some(boundary)
        && metrics.last_applied >= *membership.log_id()
}

async fn peer_metrics(
    a: &Shared,
    address: &str,
    id: u64,
) -> Option<BTreeMap<u64, openraft::RaftMetrics<u64, BasicNode>>> {
    let response = a
        .client
        .get(format!("http://{address}/admin/retirement-state"))
        .header("x-chronicle-cluster", &a.identity.cluster)
        .header("x-chronicle-recipient", id)
        .timeout(Duration::from_millis(500))
        .send()
        .await
        .ok()?
        .error_for_status()
        .ok()?;
    let metrics: BTreeMap<u64, openraft::RaftMetrics<u64, BasicNode>> =
        response.json().await.ok()?;
    metrics.values().all(|m| m.id == id).then_some(metrics)
}

/// A graceful drain needs evidence from the old process, not just the new quorum.
pub async fn retired(a: &Shared, id: u64) -> anyhow::Result<bool> {
    let state = control(a).await?;
    let Some(node) = state.nodes.get(&id).filter(|node| node.draining) else {
        return Ok(false);
    };
    if state.placements.len() != (SHARDS + 1) as usize
        || state
            .placements
            .values()
            .any(|p| !p.complete || p.voters.contains(&id))
    {
        return Ok(false);
    }
    let Some(metrics) = peer_metrics(a, &node.addr, id).await else {
        return Ok(false);
    };
    if !(0..=SHARDS).all(|shard| {
        metrics
            .get(&shard)
            .is_some_and(|m| replica_retired(&state.placements[&shard], m))
    }) {
        return Ok(false);
    }
    // Do not combine a newer peer observation with an already superseded intent.
    let latest = control(a).await?;
    Ok(latest.nodes.get(&id).is_some_and(|n| n.draining)
        && state.placements.iter().all(|(shard, p)| {
            latest.placements.get(shard).is_some_and(|now| {
                now.complete && now.generation == p.generation && !now.voters.contains(&id)
            })
        }))
}

async fn balance_leaders(
    id: u64,
    groups: &BTreeMap<u64, crate::Group>,
    state: &State,
    campaigns: &mut BTreeMap<u64, Campaign>,
) -> anyhow::Result<()> {
    // Scan every campaign before reconciliation's one-movement break. A skipped
    // shard must not retain an observation made before draining/ineligibility.
    for (shard, p) in &state.placements {
        // Place validates exactly three voters. Derive preference from replicated
        // intent; reject irrelevant groups before awaiting their storage actors.
        let preferred = p.voters.iter().nth((*shard % 3) as usize).copied();
        if !p.complete
            || preferred != Some(id)
            || state.nodes.get(&id).is_none_or(|node| node.draining)
        {
            campaigns.remove(shard);
            continue;
        }
        let group = &groups[shard];
        let matches = membership_applied(&group.store, &p.voters).await?;
        // Membership I/O can wait behind other storage jobs. Do not credit that
        // elapsed time to a term/leader sampled before the await.
        let current = group.raft.metrics().borrow().clone();
        if matches && let Some(leader) = current.current_leader.filter(|leader| *leader != id) {
            let now = Instant::now();
            let view = (p.generation, current.current_term, leader);
            let campaign = campaigns
                .entry(*shard)
                .or_insert(Campaign { view, since: now });
            if campaign.due(view, now) {
                tracing::info!(
                    shard,
                    leader,
                    term = current.current_term,
                    "preferred voter requesting native election; availability may pause"
                );
                group.raft.trigger().elect().await?;
            }
        } else {
            campaigns.remove(shard);
        }
    }
    Ok(())
}

async fn membership_applied(
    store: &chronicle_raft::storage::SqliteStore,
    voters: &BTreeSet<u64>,
) -> anyhow::Result<bool> {
    // Raft metrics describe effective (possibly uncommitted) membership. Only this
    // durable applied state can justify a completed placement or graceful removal.
    let (_, membership) = store.clone().applied_state().await?;
    Ok(membership.membership().get_joint_config().len() == 1
        && membership.membership().voter_ids().collect::<BTreeSet<_>>() == *voters)
}

fn target(shard: u64, nodes: &BTreeMap<u64, chronicle_raft::model::Node>) -> BTreeSet<u64> {
    let ids: Vec<_> = nodes.keys().copied().collect();
    let mut selected = BTreeSet::new();
    let mut zones = BTreeSet::new();
    // Stable rotation spreads replicas; first pass preserves distinct supplied failure domains.
    for distinct in [true, false] {
        for i in 0..ids.len() {
            let id = ids[(i + shard as usize) % ids.len()];
            if selected.len() == 3 {
                break;
            }
            if !distinct || !zones.contains(&nodes[&id].zone) {
                selected.insert(id);
                zones.insert(nodes[&id].zone.clone());
            }
        }
    }
    selected
}

#[cfg(test)]
mod tests {
    use super::*;
    use chronicle_raft::storage::SqliteStore;
    use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId, Membership};

    #[test]
    fn retirement_requires_applied_uniform_nonvoter_membership() {
        let log = LogId::new(CommittedLeaderId::new(3, 1), 17);
        let mut metrics = openraft::RaftMetrics::<u64, BasicNode>::new_initial(4);
        metrics.state = openraft::ServerState::Learner;
        let nodes = BTreeMap::from_iter((1..=4).map(|id| (id, BasicNode::new("unused"))));
        metrics.membership_config = std::sync::Arc::new(openraft::StoredMembership::new(
            Some(log),
            Membership::new(vec![BTreeSet::from([1, 2, 3])], nodes.clone()),
        ));
        metrics.last_applied = Some(LogId::new(CommittedLeaderId::new(3, 1), 16));
        assert!(!nonvoter_applied(&metrics, log));
        metrics.last_applied = Some(log);
        assert!(nonvoter_applied(&metrics, log));
        let later = LogId::new(CommittedLeaderId::new(3, 1), 19);
        assert!(!nonvoter_applied(&metrics, later));
        metrics.state = openraft::ServerState::Candidate;
        assert!(!nonvoter_applied(&metrics, log));
        metrics.state = openraft::ServerState::Learner;
        metrics.membership_config = std::sync::Arc::new(openraft::StoredMembership::new(
            Some(log),
            Membership::new(
                vec![BTreeSet::from([1, 3, 4]), BTreeSet::from([1, 2, 3])],
                nodes.clone(),
            ),
        ));
        assert!(!nonvoter_applied(&metrics, log));
        metrics.membership_config = std::sync::Arc::new(openraft::StoredMembership::new(
            Some(log),
            Membership::new(vec![BTreeSet::from([1, 3, 4])], nodes),
        ));
        assert!(!nonvoter_applied(&metrics, log));
    }

    #[test]
    fn untouched_retirement_needs_known_history_and_empty_recovered_state() {
        let mut p = Placement {
            complete: true,
            voters: BTreeSet::from([1, 2, 3]),
            ..Default::default()
        };
        let mut metrics = openraft::RaftMetrics::<u64, BasicNode>::new_initial(4);
        assert!(!replica_retired(&p, &metrics));
        p.replicas = Some(BTreeMap::from_iter(
            (1..=3).map(|id| (id, ReplicaHistory::MayVote)),
        ));
        assert!(replica_retired(&p, &metrics)); // Supplied only after recovery fence.
        metrics.last_log_index = Some(1);
        assert!(!replica_retired(&p, &metrics));
        metrics.last_log_index = None;
        p.replicas
            .as_mut()
            .unwrap()
            .insert(4, ReplicaHistory::MayVote);
        assert!(!replica_retired(&p, &metrics));
    }

    #[test]
    fn retirement_candidates_rotate_even_without_a_success_result() {
        let candidates = BTreeMap::from([
            (4, "stuck".into()),
            (5, "reachable".into()),
            (6, "unavailable".into()),
        ]);
        let mut cursor = 0;
        for expected in [4, 5, 6, 4, 5, 6] {
            assert_eq!(
                next_candidate(&candidates, &mut cursor).unwrap().0,
                expected
            );
        }
        assert!(next_candidate(&BTreeMap::new(), &mut cursor).is_none());
    }

    #[tokio::test]
    async fn control_discovery_survives_unavailable_original_seeds() {
        use crate::{App, Group, Network, Raft, identity, telemetry::Telemetry};
        use axum::{Json, Router, routing::get};
        use chronicle_raft::model::Node;
        use std::sync::Arc;
        use tokio::sync::{Mutex, Semaphore};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let registered = Node {
            addr: listener.local_addr().unwrap().to_string(),
            zone: "new".into(),
            draining: false,
        };
        // Distinct remote state proves discovery did not return the local hints.
        let remote = State {
            nodes: BTreeMap::from([(9, registered.clone())]),
            ..Default::default()
        };
        let router = Router::new().route("/admin/control", get(move || async { Json(remote) }));
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let temp = tempfile::tempdir().unwrap();
        let mut store = SqliteStore::open(temp.path().join("control"))
            .await
            .unwrap();
        store
            .apply(vec![Entry {
                log_id: LogId::new(CommittedLeaderId::new(1, 1), 1),
                payload: EntryPayload::Normal(Command::Register {
                    id: 4,
                    node: registered,
                }),
            }])
            .await
            .unwrap();
        let client = reqwest::Client::new();
        let raft = Raft::new(
            2,
            Arc::new(openraft::Config::default().validate().unwrap()),
            Network {
                client: client.clone(),
                cluster: "test".into(),
                group: 0,
            },
            store.clone(),
            store.clone(),
        )
        .await
        .unwrap();
        let (logs, _guard) = tracing_appender::non_blocking(std::io::sink());
        let app = Arc::new(App {
            id: 2,
            identity: identity::Identity {
                node: 2,
                cluster: "test".into(),
                genesis: true,
            },
            nodes: BTreeMap::from([(
                1,
                Node {
                    addr: "127.0.0.1:0".into(),
                    zone: "old".into(),
                    draining: true,
                },
            )]),
            groups: BTreeMap::from([(
                0,
                Group {
                    raft,
                    store: store.clone(),
                    movement: Mutex::new(()),
                },
            )]),
            client,
            admission: Arc::new(Semaphore::new(1)),
            live_admission: Arc::new(Semaphore::new(1)),
            telemetry: Arc::new(Telemetry::new(2, logs.error_counter(), String::new())),
        });
        assert_eq!(
            control(&app)
                .await
                .unwrap()
                .nodes
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            vec![9]
        );
        app.groups[&0].raft.shutdown().await.unwrap();
        server.abort();
        assert!(control(&app).await.is_err());
        drop(app);
        store.close().await;
    }

    #[tokio::test]
    async fn every_ineligible_campaign_clears_without_touching_storage() {
        use chronicle_raft::model::{Node, Placement};
        for (complete, draining, id) in [(false, false, 1), (true, true, 1), (true, false, 4)] {
            let mut state = State::default();
            state.nodes.insert(
                id,
                Node {
                    addr: "node".into(),
                    zone: "a".into(),
                    draining,
                },
            );
            let mut campaigns = BTreeMap::new();
            // Two shards preferring node1: both must reset, not only the first.
            for shard in [0, 3] {
                state.placements.insert(
                    shard,
                    Placement {
                        voters: BTreeSet::from([1, 2, 3]),
                        complete,
                        ..Default::default()
                    },
                );
                campaigns.insert(
                    shard,
                    Campaign {
                        view: (1, 2, 3),
                        since: Instant::now() - CAMPAIGN_DELAY,
                    },
                );
            }
            // No groups exist: indexing one would expose an irrelevant storage read.
            balance_leaders(id, &BTreeMap::new(), &state, &mut campaigns)
                .await
                .unwrap();
            assert!(campaigns.is_empty());
        }
    }

    #[test]
    fn campaign_waits_again_after_attempt_view_change_and_restart() {
        let start = Instant::now();
        let view = (7, 11, 3);
        let mut campaign = Campaign { view, since: start };
        assert!(!campaign.due(view, start + CAMPAIGN_DELAY - Duration::from_nanos(1)));
        assert!(campaign.due(view, start + CAMPAIGN_DELAY));
        // No result is supplied: successful, rejected and unknown attempts all
        // consume the interval before the native election call can be awaited.
        assert!(!campaign.due(view, start + CAMPAIGN_DELAY));
        assert!(campaign.due(view, start + CAMPAIGN_DELAY * 2));
        for changed in [(8, 11, 3), (8, 12, 3), (8, 12, 2)] {
            let now = campaign.since + CAMPAIGN_DELAY;
            assert!(!campaign.due(changed, now));
            assert!(!campaign.due(changed, now + CAMPAIGN_DELAY - Duration::from_nanos(1)));
            assert!(campaign.due(changed, now + CAMPAIGN_DELAY));
        }
        let restart = campaign.since + CAMPAIGN_DELAY;
        let mut campaign = Campaign {
            view,
            since: restart,
        };
        assert!(!campaign.due(view, restart));
        assert!(campaign.due(view, restart + CAMPAIGN_DELAY));
    }

    #[tokio::test]
    async fn placement_completion_requires_applied_uniform_membership() {
        let temp = tempfile::tempdir().unwrap();
        let mut store = SqliteStore::open(temp.path().join("membership"))
            .await
            .unwrap();
        let old = BTreeSet::from([1, 2, 3]);
        let target = BTreeSet::from([2, 3, 4]);
        let nodes = (1..=4)
            .map(|id| (id, BasicNode::new(format!("node-{id}"))))
            .collect::<BTreeMap<_, _>>();
        // Merely planning a target, or applying the joint configuration, cannot
        // authorize removal. Only applying the final uniform config can do so.
        for (index, configs, completed) in [
            (1, vec![old.clone()], false),
            (2, vec![old, target.clone()], false),
            (3, vec![target.clone()], true),
        ] {
            store
                .apply(vec![Entry {
                    log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
                    payload: EntryPayload::Membership(Membership::new(configs, nodes.clone())),
                }])
                .await
                .unwrap();
            assert_eq!(
                membership_applied(&store, &target).await.unwrap(),
                completed
            );
        }
        store.close().await;
        let store = SqliteStore::open_existing(temp.path().join("membership"))
            .await
            .unwrap();
        assert!(membership_applied(&store, &target).await.unwrap());
    }
}
