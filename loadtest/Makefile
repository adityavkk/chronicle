# One-command load-test rig. Requires: gcloud (authed), kubectl, go.
# ltctl.sh does the orchestration; these are thin, memorable wrappers.
# Override the experiment: make all SPEC=spec/sweep-50k.yaml
SPEC ?= spec/sweep-10k.yaml

.PHONY: help all up run down render

help: ## list targets
	@grep -hE '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | sort | awk 'BEGIN{FS=":.*?## "}{printf "  %-8s %s\n", $$1, $$2}'

all: ## up + run + ALWAYS down — the can't-miss-teardown one-shot
	./ltctl.sh all $(SPEC)

up: ## provision cluster + managed Redis + build images (idempotent)
	./ltctl.sh up

run: ## render SPEC, deploy the SUT, run the SLO-gated job, print the report
	./ltctl.sh run $(SPEC)

down: ## delete cluster + Redis (keeps Artifact Registry images)
	./ltctl.sh down

render: ## render SPEC to ./out without touching the cloud
	cd ../loadgen && go run ./cmd/render -spec ../loadtest/$(SPEC) -out ../loadtest/out
