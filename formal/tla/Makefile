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
# -metadir gives every TLC invocation its OWN scratch directory. Without it, TLC
# names states/<YYYY-MM-DD-HH-MM-SS> from the wall clock, so two fast runs that
# start in the same second (the small liveness configs finish in <1s) collide on
# the metadir and the second one aborts ("directory already exists"). $$RANDOM/$$$$
# (shell PID) are re-evaluated at each $(TLC) expansion, so each run is unique.
TLC                = $(JAVA) -XX:+UseParallelGC -cp $(JAR) tlc2.TLC -deadlock -workers auto -metadir states/run-$$$$-$$RANDOM
SANY               = $(JAVA) -cp $(JAR) tla2sany.SANY

.PHONY: tlc tlc-1x1 tlc-2x2 fault-expire fault-lease coverage parse jar clean \
        tlc38 ownership composed-on composed-off composed-on-2x2 composed-off-2x2 \
        composed-deposed liveness liveness-nofair liveness-safety liveness-sensitivity parse38 \
        parse-trace trace-validate trace-negative \
        tlc40 parse40 membership-safety membership-convergence membership-nofair \
        membership-witness leasetail leasetail-witness alloy

# The CI target (issue #37): parse, then both faithful safety configs.
tlc: jar parse tlc-1x1 tlc-2x2

# The issue #38 CI target: the standalone Ownership module, the fence-on/
# fence-off layering proof (1 slot + scaled 2/2/2), and the liveness/fairness
# encoding (incl. the no-fairness negative test and the LB-5 sensitivity check).
tlc38: jar parse38 ownership composed-on composed-off composed-on-2x2 composed-off-2x2 \
       composed-deposed liveness liveness-nofair liveness-safety liveness-sensitivity

jar:
	@test -f $(JAR) || curl -L -o $(JAR) "$(TLA_TOOLS_URL)"

parse: jar
	$(SANY) SubscriptionFence.tla

parse38: jar
	$(SANY) Ownership.tla
	$(SANY) Composed.tla
	$(SANY) Liveness.tla

# --- issue #40 targets: membership/HRW convergence + L3 lease-tail refinement -
#
# (A) The distributed membership + HRW + slot-reconcile CONVERGENCE surface
# (L2/L4/L5, INV-JEP-L4-01 / INV-HRW-01) that no exhaustive check covered:
#   membership-safety      : AtMostOneOwner / NoLiveSplitBrain / EpochMonotone /
#                            TransferBumpsEpoch over the full churn interleaving.
#   membership-convergence : under WF(reconcile)+SF(heartbeat)+WF(tick), after
#                            churn stops the system converges and STAYS converged
#                            (one unexpired HRW-target owner per slot, no oscill.).
#   membership-nofair      : negative control -- WITHOUT fairness convergence MUST
#                            FAIL (the leads-to is non-trivial).
#   membership-witness     : non-vacuity -- a genuine TRANSFER and a genuine
#                            zero-owner coverage GAP are each really reached.
# (B) The L3 lease-tail-drop refinement (INV-LR-01 / INV-JEP-L3-01): the lease
# ZSET is a cache repairable from the durable record.
#   leasetail              : NoSpuriousLease (never invent a lease) + a stranded
#                            lease is eventually restored from the durable hash.
tlc40: jar parse40 membership-safety membership-convergence membership-nofair \
       membership-witness leasetail leasetail-witness

parse40: jar
	$(SANY) Membership.tla
	$(SANY) MC_Membership.tla
	$(SANY) LeaseTail.tla

membership-safety: jar
	$(TLC) -config Membership_Safety.cfg MC_Membership.tla

membership-convergence: jar
	$(TLC) -config Membership_Convergence.cfg MC_Membership.tla

# Negative control: WITHOUT fairness the convergence leads-to MUST FAIL.
membership-nofair: jar
	@! $(TLC) -config Membership_NoFair.cfg MC_Membership.tla 2>&1 | tee /dev/stderr | grep -q 'No error has been found' \
		|| (echo "FAIL: convergence held WITHOUT fairness — property is trivial" && exit 1)

# Non-vacuity: a genuine TRANSFER and a genuine zero-owner GAP are each reached
# (each NotX invariant MUST be violated).
membership-witness: jar
	@! $(TLC) -config Membership_Witness.cfg MC_Membership.tla 2>&1 | tee /dev/stderr | grep -q 'No error has been found' \
		|| (echo "FAIL: no TRANSFER was reachable — convergence run is vacuous" && exit 1)
	@! $(TLC) -config Membership_WitnessGap.cfg MC_Membership.tla 2>&1 | tee /dev/stderr | grep -q 'No error has been found' \
		|| (echo "FAIL: no zero-owner GAP was reachable — convergence run is vacuous" && exit 1)

leasetail: jar
	$(TLC) -config LeaseTail.cfg LeaseTail.tla

# Non-vacuity: the stranded-lease state is actually reachable (NoStranded MUST
# be violated), so LeaseRecoverable is not vacuously true.
leasetail-witness: jar
	@printf 'SPECIFICATION SpecNoFair\nCONSTANTS\n Records = {r1, r2}\nINVARIANT NoStranded\n' > LeaseTail_witness.cfg
	@! $(TLC) -config LeaseTail_witness.cfg LeaseTail.tla 2>&1 | tee /dev/stderr | grep -q 'No error has been found' \
		|| (echo "FAIL: stranded-lease state not reachable — LeaseRecoverable is vacuous" && exit 1)
	@rm -f LeaseTail_witness.cfg

# (B) The Alloy relational models (INV-RECOVER-04 + INV-JEP-T5-01). The Alloy
# dist jar is downloaded on demand to /tmp by run.sh and is NOT committed. Every
# `check` must print UNSAT (the assertion holds) and every witness `run` SAT.
alloy:
	bash ../alloy/run.sh

# --- issue #38 targets --------------------------------------------------------

# Standalone Ownership owner-epoch CAS (INV-OWNER-01): 1 and 2 slots.
ownership: jar
	$(TLC) -config Ownership_1slot.cfg MC_Ownership.tla
	$(TLC) -config Ownership_2slot.cfg MC_Ownership.tla

# The layering proof (INV-OWNER-02): []SingleHolder MUST hold with the
# owner-epoch fence enforced (Real) AND with it deleted (AlwaysPass).
composed-on: jar
	$(TLC) -config Composed_FenceOn.cfg MC_Composed.tla
composed-off: jar
	$(TLC) -config Composed_FenceOff.cfg MC_Composed.tla
composed-on-2x2: jar
	$(TLC) -config Composed_FenceOn_2x2.cfg MC_ComposedSym.tla
composed-off-2x2: jar
	$(TLC) -config Composed_FenceOff_2x2.cfg MC_ComposedSym.tla

# Coverage: the Deposed-owner-late-write scenario MUST be reachable under
# AlwaysPass (so the layering run is non-vacuous).  MUST violate the witness.
composed-deposed: jar
	@! $(TLC) -config Composed_DeposedWitness.cfg MC_Composed.tla 2>&1 | tee /dev/stderr | grep -q 'No error has been found' \
		|| (echo "FAIL: Deposed-owner-late-write was NOT reachable under AlwaysPass" && exit 1)

# Liveness leads-to under weak fairness of the sweep/due loops (INV-WAKE-01,
# INV-RECOVER-01/02).
liveness: jar
	$(TLC) -config Liveness.cfg MC_Liveness.tla

# Negative test: WITHOUT fairness the leads-to MUST FAIL (property non-trivial).
liveness-nofair: jar
	@! $(TLC) -config Liveness_NoFair.cfg MC_Liveness.tla 2>&1 | tee /dev/stderr | grep -q 'No error has been found' \
		|| (echo "FAIL: leads-to held WITHOUT fairness — property is trivial" && exit 1)

# NoDoubleLiveHolder under the SlowConsumer + re-emit scenario (INV-JEP-L1-01).
liveness-safety: jar
	$(TLC) -config Liveness_Safety.cfg MC_Liveness.tla

# LB-5 sensitivity: NoDoubleLiveHolder MUST still hold at ReemitTicks=0
# (safety is threshold-independent).
liveness-sensitivity: jar
	$(TLC) -config Liveness_Sensitivity.cfg MC_Liveness.tla

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

# --- issue #39: trace validation (the CCF "smart casual verification" bridge) -
#
# parse-trace : SANY-parse the Trace.tla wrapper against a sample generated
#               TraceData so the bridge module stays well-formed in CI.
# trace-validate : drive the full pipeline — produce a real fence trace from the
#               running engine (subtrace build) against a live Redis, convert it,
#               and run TLC (DFS) to confirm every recorded trace is a legal
#               behavior of SubscriptionFence (S ∩ T ≠ ∅). Needs Redis at
#               $(CHRONICLE_REDIS_ADDR) (default localhost:6379).
CHRONICLE_REDIS_ADDR ?= localhost:6379

parse-trace: jar
	@tmp=$$(mktemp -d); jar=$$(cd $$(dirname $(JAR)) 2>/dev/null && pwd)/$$(basename $(JAR)); \
	[ -f $(JAR) ] && jar=$$(pwd)/$(JAR); \
	printf '%s\n' '---- MODULE TraceData ----' 'EXTENDS Naturals, Sequences' \
	  'NoWorker == "none"' 'TraceWorkers == { "w1", "w2" }' 'TraceMaxGen == 2' \
	  'TraceLog == << >>' '====' > $$tmp/TraceData.tla; \
	cp Trace.tla SubscriptionFence.tla $$tmp/; \
	( cd $$tmp && $(JAVA) -cp $$jar tla2sany.SANY Trace.tla ) && echo "Trace.tla parses"; rm -rf $$tmp

trace-validate: jar
	JAR=$(JAR) bash trace/validate.sh $(CHRONICLE_REDIS_ADDR)

# Non-vacuity check: an impossible (deposed-OK-ack) trace MUST be REJECTED.
trace-negative: jar
	JAR=$(JAR) bash trace/negative_control.sh $(CHRONICLE_REDIS_ADDR)

clean:
	rm -f *.jar states/ -r 2>/dev/null || true
