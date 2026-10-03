//! Deterministic API -> message -> core tests. No core loop, disk, network, or sleeps.
//! Accepted entries are explicitly committed/responded to by the harness so a term change can
//! be placed exactly between the two public API phases.

use std::ops::RangeBounds;
use std::task::Poll;

use maplit::{btreemap, btreeset};

use super::*;
use crate::engine::testing::UTConfig as C;
use crate::network::{RPCOption, RaftNetwork};
use crate::storage::LogFlushed;
use crate::testing::log_id;
use crate::utime::UTime;
use crate::{
    ChangeMembers, EffectiveMembership, Entry, Membership, MembershipState, RaftLogReader,
    RaftSnapshotBuilder, SnapshotMeta, StorageError, StoredMembership, TokioInstant,
};

// These interfaces are required to construct the real core, but admission must not execute IO.
// Every method panics to catch accidental use; engine output is checked separately below.
struct NoIo;

impl RaftNetworkFactory<C> for NoIo {
    type Network = Self;
    async fn new_client(&mut self, _: u64, _: &()) -> Self {
        panic!("unexpected network IO")
    }
}
impl RaftNetwork<C> for NoIo {
    async fn append_entries(
        &mut self,
        _: AppendEntriesRequest<C>,
        _: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, crate::error::RPCError<u64, (), RaftError<u64>>> {
        panic!("unexpected IO")
    }
    async fn vote(
        &mut self,
        _: VoteRequest<u64>,
        _: RPCOption,
    ) -> Result<VoteResponse<u64>, crate::error::RPCError<u64, (), RaftError<u64>>> {
        panic!("unexpected IO")
    }
    async fn install_snapshot(
        &mut self,
        _: InstallSnapshotRequest<C>,
        _: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        crate::error::RPCError<u64, (), RaftError<u64, crate::error::InstallSnapshotError>>,
    > {
        panic!("unexpected IO")
    }
}
impl RaftLogReader<C> for NoIo {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        _: RB,
    ) -> Result<Vec<Entry<C>>, StorageError<u64>> {
        panic!("unexpected IO")
    }
}
impl RaftLogStorage<C> for NoIo {
    type LogReader = Self;
    async fn get_log_state(&mut self) -> Result<crate::LogState<C>, StorageError<u64>> {
        panic!("unexpected IO")
    }
    async fn get_log_reader(&mut self) -> Self {
        panic!("unexpected IO")
    }
    async fn save_vote(&mut self, _: &Vote<u64>) -> Result<(), StorageError<u64>> {
        panic!("unexpected IO")
    }
    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        panic!("unexpected IO")
    }
    async fn append<I>(&mut self, _: I, _: LogFlushed<C>) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<C>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        panic!("unexpected IO")
    }
    async fn truncate(&mut self, _: LogId<u64>) -> Result<(), StorageError<u64>> {
        panic!("unexpected IO")
    }
    async fn purge(&mut self, _: LogId<u64>) -> Result<(), StorageError<u64>> {
        panic!("unexpected IO")
    }
}
impl RaftSnapshotBuilder<C> for NoIo {
    async fn build_snapshot(&mut self) -> Result<Snapshot<C>, StorageError<u64>> {
        panic!("unexpected IO")
    }
}
impl RaftStateMachine<C> for NoIo {
    type SnapshotBuilder = Self;
    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, ()>), StorageError<u64>> {
        panic!("unexpected IO")
    }
    async fn apply<I>(&mut self, _: I) -> Result<Vec<()>, StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<C>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        panic!("unexpected IO")
    }
    async fn get_snapshot_builder(&mut self) -> Self {
        panic!("unexpected IO")
    }
    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<std::io::Cursor<Vec<u8>>>, StorageError<u64>> {
        panic!("unexpected IO")
    }
    async fn install_snapshot(
        &mut self,
        _: &SnapshotMeta<u64, ()>,
        _: Box<std::io::Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        panic!("unexpected IO")
    }
    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<C>>, StorageError<u64>> {
        panic!("unexpected IO")
    }
}

type Core = RaftCore<C, NoIo, NoIo, NoIo>;

fn harness() -> (Raft<C>, Core) {
    let config = Arc::new(Config::default());
    let runtime_config = Arc::new(RuntimeConfig::new(&config));
    let (tx_api, rx_api) = mpsc::unbounded_channel();
    let (tx_notify, rx_notify) = mpsc::unbounded_channel();
    let (tx_metrics, rx_metrics) = watch::channel(RaftMetrics::new_initial(1));
    let (tx_data_metrics, rx_data_metrics) = watch::channel(RaftDataMetrics::default());
    let (tx_server_metrics, rx_server_metrics) = watch::channel(RaftServerMetrics::default());
    let mut engine = Engine::<C>::default();
    engine.state.enable_validation(false); // Same incomplete-state seam as upstream engine tests.
    engine.config.id = 1;
    engine.state.vote = UTime::new(TokioInstant::now(), Vote::new_committed(1, 1));
    engine.state.log_ids.append(log_id(1, 1, 0));
    engine.state.committed = Some(log_id(1, 1, 0));
    let membership = Arc::new(EffectiveMembership::new(
        Some(log_id(1, 1, 0)),
        Membership::new(vec![btreeset! {1}], btreeset! {1, 2}),
    ));
    engine.state.membership_state = MembershipState::new(membership.clone(), membership);
    engine.testing_new_leader();
    engine.state.server_state = engine.calc_server_state();
    engine.output.take_commands();

    let raft = Raft {
        inner: Arc::new(RaftInner {
            id: 1,
            config: config.clone(),
            runtime_config: runtime_config.clone(),
            tick_handle: Tick::spawn(Duration::from_secs(3600), tx_notify.clone(), false),
            tx_api: tx_api.clone(),
            rx_metrics,
            rx_data_metrics,
            rx_server_metrics,
            tx_shutdown: Mutex::new(None),
            core_state: Mutex::new(CoreState::Done(Err(Fatal::Stopped))),
            snapshot: Mutex::new(None),
        }),
    };
    let core = Core {
        id: 1,
        config,
        runtime_config,
        network: NoIo,
        log_store: NoIo,
        sm_handle: worker::Worker::spawn(NoIo, tx_notify.clone()),
        engine,
        client_resp_channels: BTreeMap::new(),
        replications: Default::default(),
        leader_data: None,
        tx_api,
        rx_api,
        tx_notify,
        rx_notify,
        tx_metrics,
        tx_data_metrics,
        tx_server_metrics,
        command_state: CommandState::default(),
        span: tracing::Span::none(),
        _p: Default::default(),
    };
    (raft, core)
}

fn later_term(core: &mut Core) {
    core.engine.state.vote = UTime::new(TokioInstant::now(), Vote::new_committed(2, 1));
    core.engine.testing_new_leader();
    core.engine.state.server_state = core.engine.calc_server_state();
    core.engine.output.take_commands();
}

fn complete_proposal(core: &mut Core) {
    let membership = core.engine.state.membership_state.effective().clone();
    let log_id = membership.log_id().clone().unwrap();
    core.engine.state.membership_state.commit(&Some(log_id));
    core.engine.state.committed = Some(log_id);
    core.client_resp_channels
        .remove(&log_id.index)
        .unwrap()
        .send(Ok(ClientWriteResponse {
            log_id,
            data: (),
            membership: Some(membership.membership().clone()),
        }));
    assert!(
        !core.engine.output.take_commands().is_empty(),
        "accepted entry must emit commands"
    );
}

async fn reject_without_mutation(core: &mut Core) {
    let before = format!("{:?}", core.engine.state);
    let msg = core.rx_api.try_recv().unwrap();
    core.handle_api_msg(msg).await;
    assert_eq!(before, format!("{:?}", core.engine.state));
    assert!(
        core.engine.output.take_commands().is_empty(),
        "no append or other IO command"
    );
    assert!(core.client_resp_channels.is_empty());
}

#[tokio::test]
async fn membership_admission_rejects_old_vote_after_same_node_reelection() {
    let (raft, mut core) = harness();
    let future = raft.change_membership_if_vote(
        ChangeMembers::AddNodes(btreemap! {3 => ()}),
        true,
        Vote::new_committed(1, 1),
    );
    let mut future = std::pin::pin!(future);
    assert!(futures::poll!(&mut future).is_pending()); // Already queued before reelection.
    later_term(&mut core);
    reject_without_mutation(&mut core).await;
    assert!(matches!(
        future.await,
        Err(RaftError::APIError(ClientWriteError::ForwardToLeader(_)))
    ));
}

#[tokio::test]
async fn membership_admission_compares_complete_vote() {
    for expected in [Vote::new_committed(1, 2), Vote::new(1, 1)] {
        let (raft, mut core) = harness();
        let future = raft.change_membership_if_vote(
            ChangeMembers::AddNodes(btreemap! {3 => ()}),
            true,
            expected,
        );
        let mut future = std::pin::pin!(future);
        assert!(futures::poll!(&mut future).is_pending());
        reject_without_mutation(&mut core).await;
        assert!(matches!(
            future.await,
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(_)))
        ));
    }
}

#[tokio::test]
async fn membership_admission_second_phase_keeps_original_vote() {
    let (raft, mut core) = harness();
    let future = raft.change_membership_if_vote(btreeset! {1, 2}, true, Vote::new_committed(1, 1));
    let mut future = std::pin::pin!(future);
    assert!(futures::poll!(&mut future).is_pending());
    let msg = core.rx_api.try_recv().unwrap();
    core.handle_api_msg(msg).await;
    complete_proposal(&mut core);
    assert_eq!(
        2,
        core.engine
            .state
            .membership_state
            .effective()
            .membership()
            .get_joint_config()
            .len()
    );
    later_term(&mut core);
    // Also publish the new vote, to detect an implementation that refreshes from metrics.
    core.tx_metrics
        .send_modify(|metrics| metrics.vote = Vote::new_committed(2, 1));
    assert!(futures::poll!(&mut future).is_pending());
    reject_without_mutation(&mut core).await;
    assert!(matches!(
        future.await,
        Err(RaftError::APIError(ClientWriteError::ForwardToLeader(_)))
    ));
}

#[tokio::test]
async fn membership_admission_unfenced_api_still_crosses_terms() {
    let (raft, mut core) = harness();
    let future = raft.change_membership(btreeset! {1, 2}, true);
    let mut future = std::pin::pin!(future);
    for phase in 0..2 {
        assert!(futures::poll!(&mut future).is_pending());
        let msg = core.rx_api.try_recv().unwrap();
        assert!(matches!(
            &msg,
            RaftMsg::ChangeMembership {
                expected_vote: None,
                ..
            }
        ));
        core.handle_api_msg(msg).await;
        complete_proposal(&mut core);
        if phase == 0 {
            later_term(&mut core);
        }
    }
    assert_eq!(
        1,
        future
            .await
            .unwrap()
            .membership
            .unwrap()
            .get_joint_config()
            .len()
    );
}

#[tokio::test]
async fn membership_admission_add_learner_remains_unfenced() {
    let (raft, mut core) = harness();
    let future = raft.add_learner(3, (), false);
    let mut future = std::pin::pin!(future);
    assert!(futures::poll!(&mut future).is_pending());
    let msg = core.rx_api.try_recv().unwrap();
    assert!(matches!(
        &msg,
        RaftMsg::ChangeMembership {
            expected_vote: None,
            ..
        }
    ));
    core.handle_api_msg(msg).await;
    complete_proposal(&mut core);
    assert!(matches!(futures::poll!(&mut future), Poll::Ready(Ok(_))));
}

#[tokio::test]
async fn membership_admission_matching_vote_completes_both_phases() {
    let (raft, mut core) = harness();
    let expected = Vote::new_committed(1, 1);
    let future = raft.change_membership_if_vote(btreeset! {1, 2}, true, expected);
    let mut future = std::pin::pin!(future);
    for _ in 0..2 {
        assert!(futures::poll!(&mut future).is_pending());
        let msg = core.rx_api.try_recv().unwrap();
        assert!(
            matches!(&msg, RaftMsg::ChangeMembership { expected_vote: Some(v), .. } if *v == expected)
        );
        core.handle_api_msg(msg).await;
        complete_proposal(&mut core);
    }
    assert_eq!(
        1,
        future
            .await
            .unwrap()
            .membership
            .unwrap()
            .get_joint_config()
            .len()
    );
}
