//! Local identity is deliberately outside Raft snapshots. A copied volume is not a new node.
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use chronicle_raft::model::{Node, SHARDS};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Identity {
    pub cluster: String,
    pub node: u64,
    pub genesis: bool,
}

pub struct Directory {
    path: PathBuf,
    _lock: File,
}

impl Directory {
    pub fn lock(path: &Path) -> anyhow::Result<Self> {
        fs::create_dir_all(path)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path.join("process.lock"))?;
        lock.try_lock()?;
        Ok(Self {
            path: path.into(),
            _lock: lock,
        })
    }

    pub fn require_empty(&self) -> anyhow::Result<()> {
        for entry in fs::read_dir(&self.path)? {
            anyhow::ensure!(
                entry?.file_name() == "process.lock",
                "initialization requires empty storage; never reuse a lost node identity"
            );
        }
        Ok(())
    }

    pub fn persist(&self, identity: &Identity) -> anyhow::Result<()> {
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(self.path.join("identity.json"))?;
        file.write_all(&serde_json::to_vec(identity)?)?;
        file.sync_all()?;
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }

    pub fn restart(&self, node: u64, cluster: &str) -> anyhow::Result<Identity> {
        let identity: Identity =
            serde_json::from_slice(&fs::read(self.path.join("identity.json"))?)?;
        anyhow::ensure!(
            identity.node == node && identity.cluster == cluster,
            "storage identity mismatch"
        );
        for group in 0..=SHARDS {
            anyhow::ensure!(
                self.path.join(format!("group-{group}.sqlite")).is_file(),
                "missing group {group}; restore original disk or replace with a fresh learner ID"
            );
        }
        Ok(identity)
    }
}

pub fn validate_seeds(nodes: &BTreeMap<u64, Node>) -> anyhow::Result<()> {
    let addresses: std::collections::BTreeSet<_> = nodes.values().map(|n| &n.addr).collect();
    anyhow::ensure!(
        nodes.len() == 3 && addresses.len() == 3,
        "genesis requires three distinct IDs and addresses"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restart_is_bound_to_identity_and_requires_every_store() {
        let temp = tempfile::tempdir().unwrap();
        let dir = Directory::lock(temp.path()).unwrap();
        dir.require_empty().unwrap();
        assert!(dir.restart(1, "c").is_err());
        for group in 0..=SHARDS {
            File::create(temp.path().join(format!("group-{group}.sqlite"))).unwrap();
        }
        dir.persist(&Identity {
            node: 1,
            cluster: "c".into(),
            genesis: true,
        })
        .unwrap();
        assert!(Directory::lock(temp.path()).is_err());
        assert!(dir.require_empty().is_err());
        assert!(dir.restart(1, "c").is_ok());
        assert!(dir.restart(2, "c").is_err());
        assert!(dir.restart(1, "other").is_err());
        fs::remove_file(temp.path().join("group-2.sqlite")).unwrap();
        assert!(dir.restart(1, "c").is_err());
    }

    #[test]
    fn duplicate_seed_addresses_are_not_three_replicas() {
        let mut nodes: BTreeMap<_, _> = (1..=3)
            .map(|id| {
                (
                    id,
                    Node {
                        addr: format!("node-{id}"),
                        zone: String::new(),
                        draining: false,
                    },
                )
            })
            .collect();
        validate_seeds(&nodes).unwrap();
        nodes.get_mut(&3).unwrap().addr = "node-1".into();
        assert!(validate_seeds(&nodes).is_err());
    }
}
