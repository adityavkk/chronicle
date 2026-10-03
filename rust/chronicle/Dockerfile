# Build first: cargo build --release --locked (pinned rust-toolchain.toml).
FROM debian:bookworm-slim@sha256:3783cc01769c7b2b1b83a5c5ad96c815348e28ed7da68e2e3687004faa906251
COPY target/release/chronicle-raft /usr/local/bin/chronicle-raft
USER 1000:1000
ENTRYPOINT ["/usr/local/bin/chronicle-raft"]
