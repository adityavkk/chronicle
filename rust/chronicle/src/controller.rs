//! Reconcile replicated whole-shard intents. One move at a time; retries are idempotent.
use crate::{Shared, now_ms};
use chronicle_raft::model::{Command, SHARDS, State};
use openraft::BasicNode;
use openraft::storage::RaftStateMachine;
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

pub async fn run(a: Shared) {
    loop {
        match tokio::time::timeout(Duration::from_secs(20), tick(&a)).await {
            Ok(Ok(())) => (),
            result => tracing::warn!(error = ?result, "controller retry; outcome may be unknown"),
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
    for n in a.nodes.values() {
        if let Ok(r) = a
            .client
            .get(format!("http://{}/admin/control", n.addr))
            .send()
            .await
            && r.status().is_success()
        {
            return Ok(r.json().await?);
        }
    }
    anyhow::bail!("control quorum unavailable")
}

async fn tick(a: &Shared) -> anyhow::Result<()> {
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
                            })
                            .await?;
                        break;
                    }
                }
            }
        }
    }
    state = control(a).await?;
    for (shard, p) in &state.placements {
        let group = &a.groups[shard];
        let initial = group.raft.metrics().borrow().clone();
        if initial.current_leader != Some(a.id) {
            continue;
        }
        let matches = membership_applied(&group.store, &p.voters).await?;
        if p.complete && matches {
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
            group
                .raft
                .change_membership(p.voters.clone(), false)
                .await?;
        }
        anyhow::ensure!(
            membership_applied(&group.store, &p.voters).await?,
            "target membership not applied"
        );
        // Membership commitment is authoritative even if this completion record is lost.
        // The next leader can inspect/repeat the same intent safely.
        let command = Command::Placed {
            shard: *shard,
            generation: p.generation,
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
                serde_json::to_vec(&(*shard, p.generation))?.into(),
            )
            .await
            .map_err(|(status, message)| anyhow::anyhow!("{status}: {message}"))?;
            anyhow::ensure!(
                response.status().is_success(),
                "placement completion rejected"
            );
        }
        break;
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
