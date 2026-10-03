CARGO := $(HOME)/.cargo/bin/cargo

.PHONY: check fault-check formal image local
check: fault-check
	$(CARGO) fmt --all -- --check
	$(CARGO) clippy --locked --all-targets -- -D warnings
	$(CARGO) test --locked
	RUSTDOCFLAGS='-D warnings' $(CARGO) doc --locked --no-deps
	python3 -m unittest discover -s tests -p 'test_*.py' -v

fault-check:
	$(CARGO) clippy --locked --all-targets --features storage-faults -- -D warnings
	$(CARGO) test --locked --features storage-faults --test fault_gates
	$(CARGO) fmt --manifest-path tests/vfs/Cargo.toml -- --check
	CARGO_TARGET_DIR=$(CURDIR)/target $(CARGO) clippy --manifest-path tests/vfs/Cargo.toml --locked --all-targets -- -D warnings
	CARGO_TARGET_DIR=$(CURDIR)/target $(CARGO) test --manifest-path tests/vfs/Cargo.toml --locked

formal:
	PATH="$(HOME)/.elan/bin:$$PATH" $(MAKE) -C formal check

image:
	$(CARGO) build --release --locked
	sudo docker build -t chronicle-raft:dev .

local: image
	bash ops/cluster.sh create
	NEW_GENESIS=1 bash ops/setup.sh
	bash ops/bootstrap.sh
