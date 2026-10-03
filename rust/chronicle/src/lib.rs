//! Durable-majority stream state and OpenRaft integration.
use std::io::Cursor;
pub mod model;
pub mod network;
pub mod storage;
pub mod wire;

openraft::declare_raft_types!(pub TypeConfig: D = model::Command, R = model::Outcome);
pub type Raft = openraft::Raft<TypeConfig>;
