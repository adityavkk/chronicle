//! Private directed-transfer executor. A persisted claim is never a permit to replay.
use crate::{ApiResult, Group, Shared, controller, now_ms, rpc_recipient, unavailable};
use axum::{
    Json,
    extract::{Path, State},
    http::HeaderMap,
    response::IntoResponse,
};
use chronicle_raft::{
    TypeConfig,
    balance::Load,
    leadership::{self, Attempt, EXPIRES_MS, Observation, Operation, Phase, View},
    model::{Command, State as Control},
};
use openraft::{storage::RaftStateMachine, type_config::async_runtime::WatchReceiver};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, time::Duration};

/// Pair applied membership with a stable effective identity. Metrics alone may
/// describe a joint or uncommitted configuration and cannot authorize a move.
pub async fn view(group: &Group) -> anyhow::Result<Option<View>> {
    let before = group.raft.metrics().borrow_watched().clone();
    let (applied, membership) = group.store.clone().applied_state().await?;
    let after = group.raft.metrics().borrow_watched().clone();
    if before.vote != after.vote
        || before.membership_config.log_id() != after.membership_config.log_id()
        || membership.log_id() != after.membership_config.log_id()
        || membership.membership().get_joint_config().len() != 1
        || !after.vote.committed
    {
        return Ok(None);
    }
    let voters: Vec<_> = membership.membership().voter_ids().collect();
    let (Ok(voters), Some(membership)) = (voters.try_into(), *membership.log_id()) else {
        return Ok(None);
    };
    Ok(Some(View {
        vote: after.vote,
        membership,
        voters,
        leader: after.state == openraft::ServerState::Leader,
        applied,
    }))
}

pub async fn observe(
    State(a): State<Shared>,
    Path(shard): Path<u64>,
    headers: HeaderMap,
) -> ApiResult {
    rpc_recipient(&a.identity, &headers)?;
    let group = a
        .groups
        .get(&shard)
        .ok_or_else(|| unavailable("unknown shard"))?;
    let vote = group.raft.metrics().borrow_watched().vote;
    group
        .raft
        .ensure_linearizable(openraft::ReadPolicy::ReadIndex)
        .await
        .map_err(unavailable)?;
    let view = view(group)
        .await
        .map_err(unavailable)?
        .filter(|v| v.leader && v.vote == vote && v.vote.leader_id.node_id == a.id)
        .ok_or_else(|| unavailable("target leadership changed"))?;
    // No redirect/proxy: the recipient itself performed the quorum barrier.
    Ok(Json(Observation {
        vote: view.vote,
        membership: view.membership,
    })
    .into_response())
}

#[derive(Serialize, Deserialize)]
pub struct Claim {
    pub id: u64,
    pub executor: u64,
}

pub async fn claim(
    State(a): State<Shared>,
    headers: HeaderMap,
    Json(request): Json<Claim>,
) -> ApiResult {
    rpc_recipient(&a.identity, &headers)?;
    let raft = &a.groups[&0].raft;
    let response = raft
        .client_write(Command::Leadership(Operation::Claim {
            id: request.id,
            executor: request.executor,
            now_ms: now_ms(),
        }))
        .await
        .map_err(unavailable)?;
    Ok(Json(response).into_response())
}

async fn observed(a: &Shared, state: &Control, attempt: &Attempt) -> Option<Observation> {
    let p = &attempt.proposal;
    let node = state.nodes.get(&p.target)?;
    let observation: Observation = a
        .client
        .get(format!("http://{}/admin/leadership/{}", node.addr, p.shard))
        .header("x-chronicle-cluster", &a.identity.cluster)
        .header("x-chronicle-recipient", p.target)
        .timeout(Duration::from_millis(800))
        .send()
        .await
        .ok()?
        .error_for_status()
        .ok()?
        .json()
        .await
        .ok()?;
    (observation.vote.committed
        && observation.vote > p.source_vote
        && observation.vote.leader_id.node_id == p.target
        && observation.membership == p.membership)
        .then_some(observation)
}

fn live(state: &Control, attempt: &Attempt, phase: Phase) -> bool {
    state
        .leadership
        .attempt
        .as_ref()
        .is_some_and(|current| current.id == attempt.id && current.phase == phase)
        && leadership::eligible(state, &attempt.proposal)
        && now_ms()
            .checked_sub(attempt.proposal.created_ms)
            .is_some_and(|age| age < EXPIRES_MS)
}

async fn admit_source(a: &Shared, state: &Control, attempt: &Attempt) -> anyhow::Result<bool> {
    let p = &attempt.proposal;
    let group = &a.groups[&p.shard];
    group
        .raft
        .ensure_linearizable(openraft::ReadPolicy::ReadIndex)
        .await?;
    let Some(source) = view(group).await? else {
        return Ok(false);
    };
    if !source.leader || source.vote != p.source_vote || source.membership != p.membership {
        return Ok(false);
    }
    let target = &state.nodes[&p.target];
    let resources: BTreeMap<u64, Load> = a
        .client
        .get(format!("http://{}/admin/resources", target.addr))
        .header("x-chronicle-cluster", &a.identity.cluster)
        .header("x-chronicle-recipient", p.target)
        .timeout(Duration::from_millis(800))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    if resources.values().any(|l| l.queued >= 32) {
        return Ok(false);
    }
    let Some(target) = resources.get(&p.shard).and_then(|l| l.view) else {
        return Ok(false);
    };
    if target.vote != p.source_vote
        || target.membership != p.membership
        || target.voters != source.voters
        || target.applied < source.applied
    {
        return Ok(false);
    }
    let current = group.raft.metrics().borrow_watched().clone();
    Ok(current.state == openraft::ServerState::Leader
        && current.vote == p.source_vote
        && current.membership_config.log_id() == &Some(p.membership))
}

pub async fn reconcile(a: &Shared, state: &Control, enabled: bool) -> anyhow::Result<()> {
    let Some(attempt) = state
        .leadership
        .attempt
        .as_ref()
        .filter(|_| state.leadership.pending())
    else {
        return Ok(());
    };
    let p = &attempt.proposal;
    let control = &a.groups[&0].raft;
    if control.metrics().borrow_watched().state == openraft::ServerState::Leader {
        let observation = if attempt.phase == Phase::Claimed && leadership::eligible(state, p) {
            observed(a, state, attempt).await
        } else {
            None
        };
        if observation.is_some()
            || !leadership::eligible(state, p)
            || now_ms()
                .checked_sub(p.created_ms)
                .is_some_and(|age| age >= EXPIRES_MS)
        {
            let result = control
                .client_write(Command::Leadership(Operation::Close {
                    id: attempt.id,
                    now_ms: now_ms(),
                    observed: observation,
                }))
                .await?;
            tracing::info!(attempt = attempt.id, shard = p.shard, control_log = %result.log_id,
                outcome = ?result.data.error, "leadership observation or expiry committed");
            return Ok(());
        }
    }
    if !enabled || attempt.phase != Phase::Planned || p.source_vote.leader_id.node_id != a.id {
        return Ok(());
    }
    let group = &a.groups[&p.shard];
    let Ok(_movement) = group.movement.try_lock() else {
        return Ok(());
    };
    let fresh = controller::control(a).await?;
    if !live(&fresh, attempt, Phase::Planned) || !admit_source(a, &fresh, attempt).await? {
        return Ok(());
    }

    let candidates = fresh
        .nodes
        .iter()
        .map(|(id, node)| (*id, node.clone()))
        .collect();
    let (owner, address) = crate::discover_leader(a, 0, candidates)
        .await
        .map_err(|(status, message)| anyhow::anyhow!("{status}: {message}"))?;
    // Do not retry or forward this request here. If its response is lost, the
    // optimization is spent. A later tick cannot reconstruct its permission.
    let claim: openraft::raft::ClientWriteResponse<TypeConfig> = a
        .client
        .post(format!("http://{address}/admin/leadership/claim"))
        .header("x-chronicle-cluster", &a.identity.cluster)
        .header("x-chronicle-recipient", owner)
        .json(&Claim {
            id: attempt.id,
            executor: a.id,
        })
        .timeout(Duration::from_secs(2))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    if claim.data.error.is_some() {
        return Ok(());
    }
    let fresh = controller::control(a).await?;
    if !live(&fresh, attempt, Phase::Claimed) || !admit_source(a, &fresh, attempt).await? {
        return Ok(());
    }
    tracing::info!(attempt = attempt.id, shard = p.shard, source_vote = ?p.source_vote,
        target = p.target, control_log = %claim.log_id, "one-shot leadership transfer submitted; outcome unknown");
    // Upstream has no conditional trigger. The local mutex and fresh checks
    // reduce races, but cannot cancel a command queued before a later vote.
    a.telemetry.leadership_submissions[p.shard as usize]
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    group.raft.trigger().transfer_leader(p.target).await?;
    Ok(())
}
