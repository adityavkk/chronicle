# Managed Redis 8 (Memorystore), provisioned only when provision_redis = true.
# The SUT's control plane lives in one {__ds} hash-tag slot, so a single managed
# instance is the realistic target. maxmemory-policy MUST be noeviction —
# anything else can silently truncate the durable log and chronicle warns on it.
#
# Note: appendfsync/AOF durability on Memorystore is set by the chosen
# persistence config of the managed offering, not a free-form arg as in the
# jepsen in-cluster Redis. Match production's durability tier here.
resource "google_redis_instance" "this" {
  count = var.provision_redis ? 1 : 0

  name               = "${var.cluster_name}-redis"
  region             = var.region
  tier               = var.redis_tier
  memory_size_gb     = var.redis_memory_gb
  redis_version      = var.redis_version
  authorized_network = "projects/${var.project_id}/global/networks/${var.network}"

  redis_configs = {
    "maxmemory-policy" = "noeviction"
  }
}
