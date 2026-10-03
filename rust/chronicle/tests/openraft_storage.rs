//! Upstream logical storage contracts; crash/fsync faults live in tests/vfs.
use chronicle_raft::{TypeConfig, storage::SqliteStore};
use openraft::{
    StorageError,
    testing::{StoreBuilder, Suite},
};
use std::sync::{Arc, Mutex};
use tempfile::TempDir;

#[derive(Clone, Default)]
struct Builder(Arc<Mutex<Vec<(TempDir, SqliteStore)>>>);

impl StoreBuilder<TypeConfig, SqliteStore, SqliteStore> for Builder {
    async fn build(&self) -> Result<((), SqliteStore, SqliteStore), StorageError<u64>> {
        let dir = tempfile::tempdir().unwrap();
        let store = SqliteStore::open(dir.path().join("raft.sqlite")).await?;
        // Suite has no async teardown. Retain the directories and final handles
        // until its cases finish, then await the actors before unlinking files.
        self.0.lock().unwrap().push((dir, store.clone()));
        Ok(((), store.clone(), store))
    }
}

#[test]
fn upstream_storage_contracts() {
    let builder = Builder::default();
    let result =
        Suite::<TypeConfig, SqliteStore, SqliteStore, Builder, ()>::test_all(builder.clone());
    let stores = std::mem::take(&mut *builder.0.lock().unwrap());
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        for (dir, store) in stores {
            store.close().await;
            drop(dir);
        }
    });
    result.unwrap();
}
