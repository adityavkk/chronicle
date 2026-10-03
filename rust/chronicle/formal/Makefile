TLA_VERSION := v1.7.4
TLA_SHA256 := 936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88
JAR := /tmp/tla2tools-$(TLA_VERSION).jar
TLC := java -XX:+UseParallelGC -cp $(JAR) tlc2.TLC -workers auto

.PHONY: check positive negative jar lean clean
check: jar lean positive negative

positive:
	cd tla && $(TLC) -metadir /tmp/chronicle-sse-read-$$$$ -config SseRead.cfg SseRead.tla
	cd tla && $(TLC) -metadir /tmp/chronicle-live-read-$$$$ -config LiveRead.cfg LiveRead.tla
	cd tla && $(TLC) -metadir /tmp/chronicle-campaign-$$$$ -config Campaign.cfg Campaign.tla
	cd tla && $(TLC) -metadir /tmp/chronicle-projection-$$$$ -config Projection.cfg Projection.tla
	cd tla && $(TLC) -metadir /tmp/chronicle-retention-$$$$ -config Retention.cfg RetentionScenario.tla
	cd tla && $(TLC) -metadir /tmp/chronicle-ownership-$$$$ -config Ownership.cfg Ownership.tla
	cd tla && $(TLC) -metadir /tmp/chronicle-safety-$$$$ -config Safety.cfg Chronicle.tla
	cd tla && $(TLC) -metadir /tmp/chronicle-placement-$$$$ -config Placement.cfg Chronicle.tla
	cd tla && $(TLC) -metadir /tmp/chronicle-scenario-$$$$ -config Scenario.cfg Scenario.tla
	cd tla && $(TLC) -metadir /tmp/chronicle-live-$$$$ -config Liveness.cfg Chronicle.tla

# TLC exit 12 plus the exact invariant name proves a model counterexample;
# parser/configuration failures cannot accidentally satisfy these controls.
negative:
	@mkdir -p evidence/negative
	@set -eu; check_bad() { name=$$1; inv=$$2; out=evidence/negative/$$name.txt; \
	  module=$${3:-Chronicle}; set +e; (cd tla && $(TLC) -metadir /tmp/chronicle-$$name-$$$$ -config $$name.cfg $$module.tla) >$$out 2>&1; rc=$$?; set -e; \
	  test $$rc -eq 12; grep -F "Invariant $$inv is violated." $$out >/dev/null; }; \
	check_bad BadAck AckDurable; \
	check_bad BadOwnerFence AcceptedAuthorityCurrent; \
	check_bad BadPromotion PromotionCaughtUp; \
	check_bad BadOwnership AcknowledgedRetained Ownership; \
	check_bad BadRetention OriginalFrontierReplies RetentionScenario; \
	check_bad BadProjectionRestart DataFromAuthority Projection; \
	check_bad BadProjectionIncarnation DataFromAuthority Projection; \
	check_bad BadCampaign CampaignSpacing Campaign; \
	check_bad BadLiveTimeout NoSkippedBytes LiveRead; \
	check_bad BadLiveIncarnation IncarnationBound LiveRead; \
	check_bad BadSseCoverage NoSkippedBytes SseRead; \
	check_bad BadSseIncarnation IncarnationBound SseRead

jar:
	@test -f $(JAR) || curl -fL -o $(JAR) https://github.com/tlaplus/tlaplus/releases/download/$(TLA_VERSION)/tla2tools.jar
	@echo '$(TLA_SHA256)  $(JAR)' | sha256sum -c -

lean:
	cd lean && lake build -- -DwarningAsError=true
	! grep -R -nE '\b(sorry|admit|axiom)\b' lean/ChronicleFormal*.lean lean/ChronicleFormal

clean:
	rm -rf lean/.lake tla/states
