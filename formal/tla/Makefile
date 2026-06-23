# Makefile for the SubscriptionFence TLA+ model (issue #37).
#
# TLC is run headless against both faithful configs and the two fault-injection
# configs.  Deadlock detection is OFF: the subscription protocol legitimately
# reaches quiescence (idle, caught up, no in-flight wake, budgets exhausted),
# which is a benign terminal state, not a protocol deadlock (see README.md).
#
# tla2tools.jar is pinned to a release and downloaded on demand; it is NOT
# committed (see .gitignore note in README).

TLA_TOOLS_VERSION ?= v1.7.4
TLA_TOOLS_URL     ?= https://github.com/tlaplus/tlaplus/releases/download/$(TLA_TOOLS_VERSION)/tla2tools.jar
JAR               ?= tla2tools.jar
JAVA              ?= java
TLC                = $(JAVA) -XX:+UseParallelGC -cp $(JAR) tlc2.TLC -deadlock -workers auto
SANY               = $(JAVA) -cp $(JAR) tla2sany.SANY

.PHONY: tlc tlc-1x1 tlc-2x2 fault-expire fault-lease coverage parse jar clean

# The CI target: parse, then both faithful safety configs.
tlc: jar parse tlc-1x1 tlc-2x2

jar:
	@test -f $(JAR) || curl -L -o $(JAR) "$(TLA_TOOLS_URL)"

parse: jar
	$(SANY) SubscriptionFence.tla

tlc-1x1: jar
	$(TLC) -config SubscriptionFence_1x1.cfg MC_1x1.tla

tlc-2x2: jar
	$(TLC) -config SubscriptionFence_2x2.cfg MC_2x2.tla

# Negative tests: these MUST find a counterexample.  `make fault-expire` /
# `make fault-lease` succeed (exit 0) only when TLC reports the violation.
fault-expire: jar
	@! $(TLC) -config SubscriptionFence_fault_expire.cfg MC_2x2.tla 2>&1 | tee /dev/stderr | grep -q 'No error has been found' \
		|| (echo "FAIL: expire fault did NOT violate SingleHolder" && exit 1)

fault-lease: jar
	@! $(TLC) -config SubscriptionFence_fault_lease.cfg MC_2x2.tla 2>&1 | tee /dev/stderr | grep -q 'No error has been found' \
		|| (echo "FAIL: lease fault did NOT violate NoStrandedLease" && exit 1)

# Crash-window reachability witnesses: each MUST violate its NotWx invariant.
coverage: jar
	@for W in 1 2 3 4; do \
		echo "=== Window W$$W reachability (must be VIOLATED) ==="; \
		$(TLC) -config SubscriptionFence_coverage_W$$W.cfg MC_2x2.tla 2>&1 | grep -E "Invariant NotW$$W is violated|No error has been found"; \
	done

clean:
	rm -f *.jar states/ -r 2>/dev/null || true
