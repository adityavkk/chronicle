# Memorystore for Redis Cluster — gate #2 cross-node fan-out testing.
#
# Unlike the single-shard google_redis_instance (BASIC/STANDARD_HA), this
# resource provisions a true sharded cluster: 3 shards × 1 node each.
# Stream data keys (without the {__ds} hash tag) hash to different shards,
# so each AppendWakeEvent in the fan-out path may route to a different node.
# This is the cross-node max-RTT that gate #2 measures and that the BASIC
# rig never exercised.
#
# Connectivity: google_redis_cluster uses Private Service Connect (PSC), not
# the legacy direct-peering of google_redis_instance. The PSC endpoint is
# auto-allocated in the authorized_network subnet; the discovery_endpoints
# output gives the address chronicle uses to connect.
#
# chronicle URL: redis+cluster://<discovery_endpoint_address>:6379
# (The chronicle binary parses redis+cluster:// and creates a ClusterClient.)
#
# Provision: terraform apply -var provision_gate2_cluster=true
# Teardown: terraform destroy -var provision_gate2_cluster=true  (or set false + apply)
resource "google_redis_cluster" "gate2" {
  count = var.provision_gate2_cluster ? 1 : 0

  name   = "${var.cluster_name}-gate2-redis"
  region = var.region

  # 3 shards = minimum cluster size; each shard on a separate node.
  # This guarantees cross-node routing for keys outside {__ds} hash slots.
  shard_count = 3

  # No replicas — gate2 is a single ephemeral rig, not a production HA setup.
  # Cost: ~$0.10/hr for 3 REDIS_SHARED_CORE_NANO nodes (tear down immediately).
  replica_count_per_shard = 0

  # Smallest node type — sufficient for the gate #2 fan-out measurement.
  node_type = "REDIS_SHARED_CORE_NANO"

  redis_configs = {
    # noeviction: chronicle treats eviction as data loss (durable log invariant).
    "maxmemory-policy" = "noeviction"
  }

  # PSC connectivity: the cluster endpoint is auto-allocated in this network.
  psc_configs {
    network = "projects/${var.project_id}/global/networks/${var.network}"
  }

  # Do not protect against accidental deletion — this is an ephemeral test rig.
  deletion_protection_enabled = false
}

output "gate2_redis_discovery" {
  description = "Memorystore CLUSTER discovery endpoint for chronicle (redis+cluster://addr:6379)."
  value = length(google_redis_cluster.gate2) > 0 ? (
    "redis+cluster://${google_redis_cluster.gate2[0].discovery_endpoints[0].address}:${google_redis_cluster.gate2[0].discovery_endpoints[0].port}"
  ) : ""
  sensitive = true
}
