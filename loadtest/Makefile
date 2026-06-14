# On-demand load-test rig. Requires: gcloud (authed), terraform, kubectl, docker.
# Override SPEC to pick an experiment: make render SPEC=spec/sweep-50k.yaml
SPEC ?= spec/sweep-10k.yaml
OUT  ?= out
NS   ?= chronicle-loadtest

.PHONY: help render infra-up deploy run results infra-down

help: ## list targets
	@grep -hE '^[a-z-]+:.*?## ' $(MAKEFILE_LIST) | sort | awk 'BEGIN{FS=":.*?## "}{printf "  %-12s %s\n", $$1, $$2}'

render: ## spec -> manifests + tfvars in $(OUT)
	cd ../loadgen && go run ./cmd/render -spec ../loadtest/$(SPEC) -out ../loadtest/$(OUT)
	cp $(OUT)/terraform.auto.tfvars terraform/

infra-up: ## provision the cluster + managed Redis (edit terraform.auto.tfvars first)
	cd terraform && terraform init && terraform apply

deploy: ## apply the SUT manifest and wait for rollout
	kubectl apply -f $(OUT)/sut.yaml
	kubectl -n $(NS) rollout status deploy/chronicle

run: ## run the SLO-gated sweep-scale job
	kubectl apply -f $(OUT)/job.yaml
	kubectl -n $(NS) wait --for=condition=complete --timeout=15m job -l app=sweepscale || true

results: ## print the job's JSON report
	kubectl -n $(NS) logs -l app=sweepscale --tail=-1

infra-down: ## tear the cluster + Redis down
	cd terraform && terraform destroy
