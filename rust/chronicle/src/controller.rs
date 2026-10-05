//! Reconcile replicated whole-shard intents. One move at a time; retries are idempotent.
use crate::{Shared, now_ms};
use chronicle_raft::model::{Command, Placement, ReplicaHistory, SHARDS, State};
use chronicle_raft::{LogId, TypeConfig, Vote};
use futures_util::{StreamExt, stream};
use openraft::BasicNode;
use openraft::storage::RaftStateMachine;
use openraft::type_config::async_runtime::WatchReceiver;
use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, Instant},
};

const CAMPAIGN_DELAY: Duration = Duration::from_secs(30);

async fn fenced_membership(
    raft: &chronicle_raft::Raft,
    members: impl Into<openraft::ChangeMembers<u64, BasicNode>>,
    retain: bool,
    vote: Vote,
) -> anyhow::Result<openraft::raft::ClientWriteResponse<TypeConfig>> {
    anyhow::ensure!(vote.committed, "membership requires an established leader");
    let membership = *raft.metrics().borrow_watched().membership_config.log_id();
    // Upstream preserves the leader fence across both phases and replaces
    // this effective-membership fence with the applied joint entry at flattening.
    // Even unchanged voters must go through admission and its InProgress check.
    Ok(raft
        .change_membership_if(
            members,
            retain,
            [
                openraft::raft::Precondition::CommittedLeaderId {
                    committed_leader_id: vote.leader_id,
                },
                openraft::raft::Precondition::LastMembershipLogId {
                    last_membership_log_id: membership,
                },
            ],
        )
        .await?)
}

#[cfg(test)]
#[path = "controller_retirement_tests.rs"]
mod retirement_tests;

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
    let mut balance = chronicle_raft::balance::Window::default();
    loop {
        match tokio::time::timeout(
            Duration::from_secs(20),
            tick(
                &a,
                &mut campaigns,
                campaigns_enabled,
                &mut retirement,
                &mut balance,
            ),
        )
        .await
        {
            Ok(Ok(())) => (),
            result => {
                campaigns.clear();
                balance.reset();
                tracing::warn!(error = ?result, "controller retry; outcome may be unknown");
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

async fn control(a: &Shared) -> anyhow::Result<State> {
    let g = &a.groups[&0];
    // A removed leader can retain a self leader hint after becoming Learner.
    if g.raft.metrics().borrow_watched().state == openraft::ServerState::Leader {
        g.raft
            .ensure_linearizable(openraft::ReadPolicy::ReadIndex)
            .await?;
        return Ok(g.store.read_state().await?);
    }
    // Seeds can all retire. Persisted registry entries are routing hints, never
    // authority: the destination still performs a strict read barrier.
    let mut nodes = a.nodes.clone();
    nodes.extend(g.store.read_state().await?.nodes);
    let hint = g.raft.metrics().borrow_watched().current_leader;
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
    balance: &mut chronicle_raft::balance::Window,
) -> anyhow::Result<()> {
    let mut state = control(a).await?;
    let control_group = &a.groups[&0];
    let authority = control_group.raft.metrics().borrow_watched().clone();
    if authority.state == openraft::ServerState::Leader {
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
        // Health is only a placement preference; it never authorizes lowering quorum.
        let healthy = healthy_nodes(&a.client, &state.nodes).await;
        if let Some(command) = next_placement(&state, &healthy, now_ms()) {
            balance.reset();
            let response = control_group.raft.client_write(command).await?;
            anyhow::ensure!(response.data.error.is_none(), "placement intent rejected");
        } else if let Some(command) =
            resource_balance(a, &state, &healthy, authority.current_term, balance).await
        {
            tracing::info!(intent = ?command, "resource placement proposed");
            let response = control_group.raft.client_write(command).await?;
            anyhow::ensure!(
                response.data.error.is_none(),
                "resource placement intent rejected"
            );
            tracing::info!(control_log = %response.log_id, "resource placement intent committed");
        }
    } else {
        balance.reset();
    }
    state = control(a).await?;
    if campaigns_enabled {
        balance_leaders(a.id, &a.groups, &state, campaigns).await?;
    }
    for (shard, p) in &state.placements {
        let group = &a.groups[shard];
        let initial = group.raft.metrics().borrow_watched().clone();
        if initial.state != openraft::ServerState::Leader {
            continue;
        }
        let matches = membership_applied(&group.store, &p.voters).await?;
        if p.retirement_known() && matches {
            continue;
        }
        let _guard = group.movement.lock().await;
        let boundary = group
            .raft
            .ensure_linearizable(openraft::ReadPolicy::ReadIndex)
            .await?;
        for id in &p.voters {
            if *id == a.id {
                continue;
            }
            let node = &state.nodes[id];
            if !matches {
                // Repeat even for registered learners: presence does not prove catch-up.
                let added = fenced_membership(
                    &group.raft,
                    openraft::ChangeMembers::AddNodes(BTreeMap::from([(
                        *id,
                        BasicNode::new(node.addr.clone()),
                    )])),
                    true,
                    initial.vote,
                )
                .await?;
                let catch_up = Some(*boundary.log_id()).max(Some(added.log_id));
                loop {
                    let m = group.raft.metrics().borrow_watched().clone();
                    anyhow::ensure!(
                        m.vote == initial.vote && m.current_leader == Some(a.id),
                        "leadership changed during catch-up"
                    );
                    if m.replication
                        .as_ref()
                        .and_then(|r| r.get(id))
                        .is_some_and(|matched| *matched >= catch_up)
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
            group.raft.metrics().borrow_watched().vote == initial.vote,
            "leadership changed"
        );
        let membership = Some(commit_membership(group, &p.voters, initial.vote).await?);
        // Membership commitment is authoritative even if this completion record is lost.
        // The next leader can inspect/repeat the same intent safely.
        let command = Command::Placed {
            shard: *shard,
            generation: p.generation,
            membership,
        };
        if control_group.raft.metrics().borrow_watched().state == openraft::ServerState::Leader {
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
            if a.groups[&shard].raft.metrics().borrow_watched().state
                == openraft::ServerState::Leader
            {
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
    let initial = group.raft.metrics().borrow_watched().clone();
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
        .filter(|(id, _)| !p.voters.contains(id))
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
    let peer = if id == a.id {
        Some(initial.clone())
    } else {
        peer_metrics(a, &address, id)
            .await
            .and_then(|mut metrics| metrics.remove(&shard))
    };
    let verified = peer.as_ref().is_some_and(|m| replica_retired(p, m));
    // OpenRaft retains a demoted leader until its node record is removed. It
    // must apply demotion first, but requiring Learner here would deadlock.
    let self_demoted = id == a.id
        && p.replicas.as_ref().is_some_and(|replicas| {
            matches!(replicas.get(&id), Some(ReplicaHistory::NonvoterAfter(boundary))
                if demotion_applied(&initial, *boundary))
        });
    if retained {
        if peer.as_ref().is_some_and(|m| m.running_state.is_ok()) && !verified && !self_demoted {
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
            && group.raft.metrics().borrow_watched().vote == initial.vote
            && group.raft.metrics().borrow_watched().current_leader == Some(a.id),
        "retirement intent or leadership changed"
    );
    if retained {
        // Removing an unreachable learner frees replication resources but is NOT
        // evidence of local retirement. The registry preserves future discovery.
        fenced_membership(
            &group.raft,
            openraft::ChangeMembers::RemoveNodes(BTreeSet::from([id])),
            false,
            initial.vote,
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
        fenced_membership(
            &group.raft,
            openraft::ChangeMembers::AddNodes(BTreeMap::from([(id, BasicNode::new(address))])),
            true,
            initial.vote,
        )
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

fn replica_retired(p: &Placement, metrics: &openraft::RaftMetrics<TypeConfig>) -> bool {
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
                && metrics.vote == Vote::new(0, metrics.id)
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

fn nonvoter_applied(metrics: &openraft::RaftMetrics<TypeConfig>, boundary: LogId) -> bool {
    metrics.state == openraft::ServerState::Learner && demotion_applied(metrics, boundary)
}

fn demotion_applied(metrics: &openraft::RaftMetrics<TypeConfig>, boundary: LogId) -> bool {
    let membership = &metrics.membership_config;
    metrics.running_state.is_ok()
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
) -> Option<BTreeMap<u64, openraft::RaftMetrics<TypeConfig>>> {
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
    let metrics: BTreeMap<u64, openraft::RaftMetrics<TypeConfig>> = response.json().await.ok()?;
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
        let current = group.raft.metrics().borrow_watched().clone();
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
                group.raft.trigger().elect(false).await?;
            }
        } else {
            campaigns.remove(shard);
        }
    }
    Ok(())
}

async fn commit_membership(
    group: &crate::Group,
    voters: &BTreeSet<u64>,
    vote: Vote,
) -> anyhow::Result<LogId> {
    // A read barrier and matching applied voters do not rule out an outstanding
    // membership entry from a cancelled call. Always await a membership operation
    // before completing a new intent; InProgress leaves the intent incomplete.
    let response = fenced_membership(&group.raft, voters.clone(), true, vote).await?;
    anyhow::ensure!(
        membership_applied(&group.store, voters).await?,
        "target membership not applied"
    );
    Ok(response.log_id)
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

async fn healthy_nodes(
    client: &reqwest::Client,
    nodes: &BTreeMap<u64, chronicle_raft::model::Node>,
) -> BTreeMap<u64, chronicle_raft::model::Node> {
    // At the 128-identity registry bound, serial 500ms probes can exhaust every
    // 20s controller tick. Eight concurrent probes leave time for reconciliation.
    let eligible: Vec<_> = nodes
        .iter()
        .filter(|(_, node)| !node.draining)
        .map(|(id, node)| (*id, node.clone()))
        .collect();
    stream::iter(eligible)
        .map(|(id, node)| async move {
            client
                .get(format!("http://{}/healthz", node.addr))
                .timeout(Duration::from_millis(500))
                .send()
                .await
                .is_ok_and(|response| response.status().is_success())
                .then_some((id, node))
        })
        .buffer_unordered(8)
        .filter_map(std::future::ready)
        .collect()
        .await
}

fn can_sample(state: &State, healthy: &BTreeMap<u64, chronicle_raft::model::Node>) -> bool {
    state.placements.len() == (SHARDS + 1) as usize
        && state.placements.values().all(|p| p.complete)
        && state
            .nodes
            .iter()
            .all(|(id, n)| n.draining || healthy.contains_key(id))
}

async fn resource_balance(
    a: &Shared,
    state: &State,
    healthy: &BTreeMap<u64, chronicle_raft::model::Node>,
    term: u64,
    window: &mut chronicle_raft::balance::Window,
) -> Option<Command> {
    if !can_sample(state, healthy) {
        window.reset();
        return None;
    }
    let loads = resource_loads(a, healthy).await;
    let current = a.groups[&0].raft.metrics().borrow_watched().clone();
    if current.state != openraft::ServerState::Leader || current.current_term != term {
        window.reset();
        return None;
    }
    window.observe(state, loads, term, Instant::now(), now_ms())
}

async fn resource_loads(
    a: &Shared,
    nodes: &BTreeMap<u64, chronicle_raft::model::Node>,
) -> chronicle_raft::balance::Loads {
    let addresses: Vec<_> = nodes
        .iter()
        .map(|(id, node)| (*id, node.addr.clone()))
        .collect();
    stream::iter(addresses)
        .map(|(id, addr)| async move {
            let response = a
                .client
                .get(format!("http://{addr}/admin/resources"))
                .header("x-chronicle-cluster", &a.identity.cluster)
                .header("x-chronicle-recipient", id)
                .timeout(Duration::from_millis(500))
                .send()
                .await
                .ok()?;
            Some((id, response.error_for_status().ok()?.json().await.ok()?))
        })
        .buffer_unordered(8)
        .filter_map(std::future::ready)
        .collect()
        .await
}

fn next_placement(
    state: &State,
    healthy: &BTreeMap<u64, chronicle_raft::model::Node>,
    now: u64,
) -> Option<Command> {
    if healthy.len() < 3 {
        return None;
    }
    let pending = state.placements.iter().find(|(_, p)| !p.complete);
    for shard in 0..=SHARDS {
        if pending.is_some_and(|(id, _)| *id != shard) {
            continue;
        }
        let old = state.placements.get(&shard);
        // A slow but eligible target retains its intent. New-node arrivals are
        // not a reason to supersede catch-up and repeatedly restart movement.
        if old.is_some_and(|p| p.voters.iter().all(|id| healthy.contains_key(id))) {
            continue;
        }
        let voters = target(shard, healthy);
        if old.is_some_and(|p| p.voters == voters || now.saturating_sub(p.changed_ms) < 15_000) {
            continue;
        }
        return Some(Command::Place {
            shard,
            expected_generation: old.map_or(0, |p| p.generation),
            voters,
            now_ms: now,
            eligible_only: true,
            repair_pending: old.is_some_and(|p| !p.complete),
        });
    }
    None
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
            let zone = nodes[&id].failure_domain();
            if !distinct || zone.is_some_and(|z| !zones.contains(z)) {
                selected.insert(id);
                if let Some(zone) = zone {
                    zones.insert(zone);
                }
            }
        }
    }
    selected
}

#[cfg(test)]
mod tests {
    use super::*;
    use chronicle_raft::Entry;
    use chronicle_raft::storage::SqliteStore;
    use openraft::vote::{RaftLeaderId, leader_id_adv::CommittedLeaderId};
    use openraft::{EntryPayload, Membership};

    #[test]
    fn persisted_advanced_leader_ids_keep_term_and_node() {
        let log_json = serde_json::json!({"leader_id": {"term": 7, "node_id": 3}, "index": 42});
        let vote_json =
            serde_json::json!({"leader_id": {"term": 7, "node_id": 3}, "committed": true});
        let log: LogId = serde_json::from_value(log_json.clone()).unwrap();
        let vote: Vote = serde_json::from_value(vote_json.clone()).unwrap();
        assert_eq!(log, LogId::new(CommittedLeaderId::new(7, 3), 42));
        assert_eq!(vote, Vote::new_committed(7, 3));
        assert_ne!(vote, Vote::new_committed(7, 2));
        assert_eq!(serde_json::to_value(log).unwrap(), log_json);
        assert_eq!(serde_json::to_value(vote).unwrap(), vote_json);
    }

    #[test]
    fn unspecified_domain_does_not_displace_available_distinct_domains() {
        let mut nodes = BTreeMap::new();
        for (id, zone) in [(1, "a"), (2, "unknown"), (3, "b"), (4, "c")] {
            nodes.insert(
                id,
                chronicle_raft::model::Node {
                    addr: id.to_string(),
                    zone: zone.into(),
                    draining: false,
                },
            );
        }
        assert_eq!(target(1, &nodes), [1, 3, 4].into());
        nodes.remove(&4);
        assert_eq!(target(1, &nodes), [1, 2, 3].into());
    }

    #[test]
    fn resource_moves_are_not_rotated_back_and_pending_repair_skips_sampling() {
        let mut state = State::default();
        for id in 1..=4 {
            state.nodes.insert(
                id,
                chronicle_raft::model::Node {
                    addr: id.to_string(),
                    zone: id.to_string(),
                    draining: false,
                },
            );
        }
        for shard in 0..=SHARDS {
            state.placements.insert(
                shard,
                Placement {
                    generation: 2,
                    voters: [1, 3, 4].into(),
                    complete: true,
                    ..Default::default()
                },
            );
        }
        assert!(can_sample(&state, &state.nodes));
        assert!(next_placement(&state, &state.nodes, 100_000).is_none());
        let mut healthy = state.nodes.clone();
        healthy.remove(&4);
        assert!(!can_sample(&state, &healthy));
        assert!(matches!(
            next_placement(&state, &healthy, 100_000),
            Some(Command::Place { shard: 0, .. })
        ));
        state.placements.get_mut(&2).unwrap().complete = false;
        assert!(!can_sample(&state, &state.nodes));
        assert!(matches!(
            next_placement(&state, &healthy, 100_000),
            Some(Command::Place {
                shard: 2,
                repair_pending: true,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn failed_health_probes_leave_budget_for_reconciliation() {
        let mut nodes = BTreeMap::new();
        let mut silent = Vec::new();
        let mut servers = tokio::task::JoinSet::new();
        for id in 1..=128 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            nodes.insert(
                id,
                chronicle_raft::model::Node {
                    addr: listener.local_addr().unwrap().to_string(),
                    zone: "a".into(),
                    draining: id == 128,
                },
            );
            if id <= 124 {
                // TCP connects but no HTTP response arrives: each probe really
                // consumes its timeout rather than failing at connection setup.
                silent.push(listener);
            } else {
                let router = axum::Router::new().route(
                    "/healthz",
                    axum::routing::get(|| async { axum::http::StatusCode::OK }),
                );
                servers.spawn(async move { axum::serve(listener, router).await.unwrap() });
            }
        }
        let healthy = tokio::time::timeout(
            Duration::from_secs(12),
            healthy_nodes(&reqwest::Client::new(), &nodes),
        )
        .await
        .unwrap();
        assert_eq!(healthy.keys().copied().collect::<Vec<_>>(), [125, 126, 127]);
        servers.abort_all();
        while servers.join_next().await.is_some() {}
    }

    #[test]
    fn pending_repair_waits_for_cooldown_and_does_not_start_other_moves() {
        let mut state = State::default();
        for id in 1..=4 {
            state.nodes.insert(
                id,
                chronicle_raft::model::Node {
                    addr: format!("node-{id}"),
                    zone: "a".into(),
                    draining: false,
                },
            );
        }
        state.placements.insert(
            2,
            Placement {
                generation: 7,
                voters: [1, 2, 4].into(),
                complete: false,
                changed_ms: 100,
                ..Default::default()
            },
        );
        // Healthy pending work is not superseded just to rebalance. Missing
        // earlier shards must not bypass it either.
        assert!(next_placement(&state, &state.nodes, 15_100).is_none());
        let mut healthy = state.nodes.clone();
        healthy.remove(&4);
        for now in [0, 15_099] {
            assert!(next_placement(&state, &healthy, now).is_none());
        }
        let Some(Command::Place {
            shard,
            expected_generation,
            voters,
            repair_pending,
            eligible_only,
            ..
        }) = next_placement(&state, &healthy, 15_100)
        else {
            panic!("pending repair should be eligible at the cooldown boundary");
        };
        assert_eq!((shard, expected_generation), (2, 7));
        assert_eq!(voters, [1, 2, 3].into());
        assert!(repair_pending && eligible_only);
        healthy.remove(&3);
        assert!(next_placement(&state, &healthy, 15_100).is_none());
    }

    #[tokio::test]
    async fn matching_prefix_cannot_complete_over_cancelled_membership() {
        use axum::{Json, Router, http::StatusCode, routing::post};
        use chronicle_raft::{Raft, TypeConfig, network::Network};
        use openraft::raft::AppendEntriesRequest;
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };

        tokio::time::timeout(Duration::from_secs(10), async {
            let dir = tempfile::tempdir().unwrap();
            let client = reqwest::Client::new();
            let mut groups = Vec::new();
            for id in [1, 2] {
                let store = SqliteStore::open(dir.path().join(format!("{id}.sqlite")))
                    .await
                    .unwrap();
                let raft = Raft::new(
                    id,
                    Arc::new(openraft::Config::default().validate().unwrap()),
                    Network {
                        client: client.clone(),
                        cluster: "membership-test".into(),
                        group: 1,
                    },
                    store.clone(),
                    store.clone(),
                )
                .await
                .unwrap();
                groups.push(crate::Group {
                    raft,
                    store,
                    movement: tokio::sync::Mutex::new(()),
                });
            }
            let blocked = Arc::new(AtomicBool::new(false));
            let peer = groups[1].raft.clone();
            let gate = blocked.clone();
            // Real follower and SQLite, with data-bearing RPCs selectively dropped.
            // Empty read-barrier heartbeats still reach the follower's real core.
            let router = Router::new().route(
                "/raft/1/append",
                post(
                    move |Json(request): Json<AppendEntriesRequest<TypeConfig>>| {
                        let peer = peer.clone();
                        let gate = gate.clone();
                        async move {
                            if gate.load(Ordering::SeqCst) && !request.entries.is_empty() {
                                return Err(StatusCode::SERVICE_UNAVAILABLE);
                            }
                            Ok(Json(peer.append_entries(request).await))
                        }
                    },
                ),
            );
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap().to_string();
            let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
            let group = &groups[0];
            group
                .raft
                .initialize(BTreeMap::from([(1, BasicNode::new("unused"))]))
                .await
                .unwrap();
            group
                .raft
                .wait(Some(Duration::from_secs(3)))
                .state(openraft::ServerState::Leader, "bootstrap")
                .await
                .unwrap();
            group
                .raft
                .ensure_linearizable(openraft::ReadPolicy::ReadIndex)
                .await
                .unwrap();
            let vote = group.raft.metrics().borrow_watched().vote;
            // A no-op voter set must not bypass admission, and leader identity
            // includes the node ID, not just its term.
            let before = group.raft.metrics().borrow_watched().last_log_index;
            let wrong_leader = Vote::new_committed(vote.leader_id.term, 2);
            let error = commit_membership(group, &BTreeSet::from([1]), wrong_leader)
                .await
                .unwrap_err();
            assert!(matches!(
                error.downcast_ref::<openraft::error::RaftError<
                    TypeConfig,
                    openraft::error::ClientWriteError<TypeConfig>,
                >>(),
                Some(openraft::error::RaftError::APIError(
                    openraft::error::ClientWriteError::PreconditionFailed(_)
                ))
            ));
            assert_eq!(group.raft.metrics().borrow_watched().last_log_index, before);
            let added = fenced_membership(
                &group.raft,
                openraft::ChangeMembers::AddNodes(BTreeMap::from([(2, BasicNode::new(address))])),
                true,
                vote,
            )
            .await
            .unwrap();
            group
                .raft
                .wait(Some(Duration::from_secs(3)))
                .metrics(
                    |m| {
                        m.replication
                            .as_ref()
                            .and_then(|r| r.get(&2))
                            .is_some_and(|matched| *matched >= Some(added.log_id))
                    },
                    "learner caught up",
                )
                .await
                .unwrap();
            blocked.store(true, Ordering::SeqCst);
            assert!(
                tokio::time::timeout(
                    Duration::from_millis(100),
                    fenced_membership(&group.raft, BTreeSet::from([1, 2]), true, vote),
                )
                .await
                .is_err()
            ); // Cancels waiter, not the outstanding joint entry.
            assert_eq!(
                group
                    .raft
                    .metrics()
                    .borrow_watched()
                    .membership_config
                    .membership()
                    .get_joint_config()
                    .len(),
                2
            );
            let original = BTreeSet::from([1]);
            assert!(membership_applied(&group.store, &original).await.unwrap());
            group
                .raft
                .ensure_linearizable(openraft::ReadPolicy::ReadIndex)
                .await
                .unwrap();
            let error = commit_membership(group, &original, vote).await.unwrap_err();
            let Some(openraft::error::RaftError::APIError(
                openraft::error::ClientWriteError::ChangeMembershipError(
                    openraft::error::ChangeMembershipError::InProgress(_),
                ),
            )) = error.downcast_ref::<openraft::error::RaftError<
                TypeConfig,
                openraft::error::ClientWriteError<TypeConfig>,
            >>()
            else {
                panic!("expected outstanding-membership rejection, got {error:?}");
            };
            blocked.store(false, Ordering::SeqCst);
            group
                .raft
                .wait(Some(Duration::from_secs(3)))
                .metrics(
                    |m| m.last_applied > Some(added.log_id),
                    "joint entry applied",
                )
                .await
                .unwrap();
            let boundary = commit_membership(group, &original, vote).await.unwrap();
            assert!(boundary > added.log_id);
            assert!(membership_applied(&group.store, &original).await.unwrap());
            let (_, applied) = group.store.clone().applied_state().await.unwrap();
            assert_eq!(*applied.log_id(), Some(boundary));
            for group in groups {
                group.raft.shutdown().await.unwrap();
                group.store.close().await;
            }
            server.abort();
        })
        .await
        .unwrap();
    }

    #[test]
    fn retirement_requires_applied_uniform_nonvoter_membership() {
        let log = LogId::new(CommittedLeaderId::new(3, 1), 17);
        let mut metrics = openraft::RaftMetrics::<TypeConfig>::new_initial(4);
        metrics.state = openraft::ServerState::Learner;
        let nodes = BTreeMap::from_iter((1..=4).map(|id| (id, BasicNode::new("unused"))));
        metrics.membership_config = std::sync::Arc::new(openraft::StoredMembership::new(
            Some(log),
            Membership::new(vec![BTreeSet::from([1, 2, 3])], nodes.clone()).unwrap(),
        ));
        metrics.last_applied = Some(LogId::new(CommittedLeaderId::new(3, 1), 16));
        assert!(!nonvoter_applied(&metrics, log));
        assert!(!demotion_applied(&metrics, log));
        metrics.last_applied = Some(log);
        assert!(nonvoter_applied(&metrics, log));
        metrics.state = openraft::ServerState::Leader;
        metrics.current_leader = Some(4);
        assert!(demotion_applied(&metrics, log));
        assert!(!nonvoter_applied(&metrics, log));
        // Removal eligibility must not become proof that leadership has ended.
        metrics.state = openraft::ServerState::Learner;
        assert!(nonvoter_applied(&metrics, log));
        let later = LogId::new(CommittedLeaderId::new(3, 1), 19);
        assert!(!nonvoter_applied(&metrics, later));
        assert!(!demotion_applied(&metrics, later));
        metrics.state = openraft::ServerState::Candidate;
        assert!(!nonvoter_applied(&metrics, log));
        metrics.state = openraft::ServerState::Learner;
        metrics.membership_config = std::sync::Arc::new(openraft::StoredMembership::new(
            Some(log),
            Membership::new(
                vec![BTreeSet::from([1, 3, 4]), BTreeSet::from([1, 2, 3])],
                nodes.clone(),
            )
            .unwrap(),
        ));
        assert!(!nonvoter_applied(&metrics, log));
        assert!(!demotion_applied(&metrics, log));
        metrics.membership_config = std::sync::Arc::new(openraft::StoredMembership::new(
            Some(log),
            Membership::new(vec![BTreeSet::from([1, 3, 4])], nodes).unwrap(),
        ));
        assert!(!nonvoter_applied(&metrics, log));
        assert!(!demotion_applied(&metrics, log));
    }

    #[test]
    fn untouched_retirement_needs_known_history_and_empty_recovered_state() {
        let mut p = Placement {
            complete: true,
            voters: BTreeSet::from([1, 2, 3]),
            ..Default::default()
        };
        let mut metrics = openraft::RaftMetrics::<TypeConfig>::new_initial(4);
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
            .apply_entries(vec![Entry {
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
            stream_tenant: None,
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
                .apply_entries(vec![Entry {
                    log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
                    payload: EntryPayload::Membership(
                        Membership::new(configs, nodes.clone()).unwrap(),
                    ),
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
