//! A terminal Raft storage error fails the shared node/PVC, not just one group.
use chronicle_raft::TypeConfig;
use openraft::type_config::{alias::WatchReceiverOf, async_runtime::WatchReceiver};
use openraft::{RaftMetrics, StorageError, error::Fatal};

pub fn storage_error(metrics: &RaftMetrics<TypeConfig>) -> Option<&StorageError<TypeConfig>> {
    match &metrics.running_state {
        Err(Fatal::StorageError(error)) => Some(error),
        _ => None,
    }
}

async fn wait(
    mut metrics: WatchReceiverOf<TypeConfig, RaftMetrics<TypeConfig>>,
) -> Option<StorageError<TypeConfig>> {
    loop {
        if let Some(error) = storage_error(&metrics.borrow_and_update()) {
            return Some(error.clone());
        }
        if metrics.changed().await.is_err() {
            return None;
        }
    }
}

pub async fn monitor(
    node: u64,
    group: u64,
    metrics: WatchReceiverOf<TypeConfig, RaftMetrics<TypeConfig>>,
) {
    if let Some(error) = wait(metrics).await {
        tracing::error!(node, group, %error, "fatal Raft storage failure; terminating node; preserving storage");
        // Runtime teardown can wait forever for an unrelated blocking file read.
        // Do not wait for destructors, consensus shutdown, or telemetry flushing.
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::{WatchSender, type_config::TypeConfigExt};

    fn fatal() -> RaftMetrics<TypeConfig> {
        let mut metrics = RaftMetrics::new_initial(1);
        metrics.running_state = Err(Fatal::StorageError(StorageError::from_io_error(
            openraft::ErrorSubject::Store,
            openraft::ErrorVerb::Write,
            std::io::Error::other("injected"),
        )));
        metrics
    }

    #[tokio::test]
    async fn initial_and_final_storage_errors_are_observed() {
        let (tx, rx) = TypeConfig::watch_channel(fatal());
        assert!(wait(rx).await.is_some());
        drop(tx);
        let (tx, rx) = TypeConfig::watch_channel(RaftMetrics::new_initial(1));
        let waiting = tokio::spawn(wait(rx));
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        tx.send(fatal()).unwrap();
        drop(tx);
        assert!(waiting.await.unwrap().is_some());
    }

    #[tokio::test]
    async fn non_storage_states_are_not_storage_failure() {
        let mut metrics = RaftMetrics::new_initial(1);
        assert!(storage_error(&metrics).is_none());
        metrics.current_term = 20;
        metrics.current_leader = None;
        assert!(storage_error(&metrics).is_none());
        for error in [Fatal::Stopped, Fatal::Panicked] {
            metrics.running_state = Err(error);
            let (tx, rx) = TypeConfig::watch_channel(metrics.clone());
            drop(tx);
            assert!(wait(rx).await.is_none());
        }
    }
}

#[cfg(all(test, feature = "storage-faults"))]
#[path = "failure_tests.rs"]
mod subprocess;
