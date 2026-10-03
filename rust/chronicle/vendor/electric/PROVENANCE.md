# Electric upstream source, unchanged

Source: electric-sql/electric packages/durable-streams-rust release 0.1.5,
commit 88793e76595d69be300731b9b25c58538923a53b.
Archive: https://static.crates.io/crates/durable-streams/durable-streams-0.1.5.crate
SHA256: 472721e61ca191520c5e2b9bf8859aa8f9aa85599027f9103addbf695724b8ad

src/ and Cargo.toml.upstream are byte-for-byte copies from that archive. LICENSE
is the Electric repository's Apache-2.0 root license at the same commit. Neither
the repository root nor this package has a NOTICE file. Copyright and license
remain with upstream; this directory is reference source, not a second server.

src/wire.rs in Chronicle extracts offset grammar and encode_wire; its header
identifies changes. We deliberately do not reuse Electric's Store/WAL authority.
Single-node local fsync and replicated commitment are different contracts.
