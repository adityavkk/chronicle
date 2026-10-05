//! Durable-majority stream state and OpenRaft integration.
pub mod balance;
pub mod expiry;
#[cfg(feature = "storage-faults")]
pub mod faults;
pub mod fork;
pub mod leadership;
pub mod metrics;
pub mod model;
pub mod network;
mod projection;
pub mod sse_wire;
pub mod storage;
pub mod wire;

openraft::declare_raft_types!(pub TypeConfig: D = model::Command, R = model::Outcome);
pub type Raft = openraft::Raft<TypeConfig, storage::SqliteStore>;
pub type Entry = openraft::type_config::alias::EntryOf<TypeConfig>;
pub type LogId = openraft::type_config::alias::LogIdOf<TypeConfig>;
pub type Vote = openraft::type_config::alias::VoteOf<TypeConfig>;
pub type Membership = openraft::type_config::alias::StoredMembershipOf<TypeConfig>;
pub type SnapshotMeta = openraft::type_config::alias::SnapshotMetaOf<TypeConfig>;
pub type Snapshot = openraft::type_config::alias::SnapshotOf<TypeConfig, std::io::Cursor<Vec<u8>>>;
