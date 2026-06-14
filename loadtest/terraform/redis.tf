# Managed Redis (Memorystore for Redis), provisioned only when provision_redis =
# true. chronicle needs only Redis 6.0+, so REDIS_7_2 works; to match production's
# managed Redis 8 (Memorystore for Valkey 8.0) set provision_redis = false and
# point sut.redis_url at it. The control plane lives in one {__ds} hash-tag slot,
# so a single managed instance is the realistic target.
#
# maxmemory-policy MUST be noeviction — chronicle warns at boot otherwise, and
# any eviction policy silently truncates the durable log. AOF/persistence
# durability is set by the managed tier (match production's), not a free-form arg
# as in the jepsen in-cluster Redis.
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
