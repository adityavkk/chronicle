variable "project_id" {
  type        = string
  description = "GCP project id the ephemeral load-test cluster lives in."
}

variable "region" {
  type    = string
  default = "us-central1"
}

variable "cluster_name" {
  type    = string
  default = "chronicle-loadtest"
}

variable "network" {
  type    = string
  default = "default"
}

# Three node pools so the load generators and observability stack never share a
# node with the system under test — co-locating them is the classic way to get
# fake numbers. Workloads pin to a pool via nodeSelector role=<pool>.
variable "sut_machine_type" {
  type    = string
  default = "e2-standard-4"
}

variable "sut_node_count" {
  type    = number
  default = 3
}

variable "loadgen_machine_type" {
  type    = string
  default = "e2-standard-8"
}

variable "loadgen_node_count" {
  type    = number
  default = 2
}

variable "obs_machine_type" {
  type    = string
  default = "e2-standard-2"
}

variable "obs_node_count" {
  type    = number
  default = 1
}

# Managed Redis 8. By default the rig provisions a fresh Memorystore instance for
# the test. To measure against the SAME managed Redis 8 production uses (so the
# numbers transfer), set provision_redis = false and put that instance's URL in
# the experiment spec's sut.redis_url instead.
variable "provision_redis" {
  type    = bool
  default = true
}

variable "redis_tier" {
  type    = string
  default = "STANDARD_HA"
}

variable "redis_memory_gb" {
  type    = number
  default = 5
}

variable "redis_version" {
  type    = string
  default = "REDIS_7_2"
  description = <<-EOT
    Memorystore engine version. Set this to your standardized managed Redis 8
    offering. REDIS_7_2 is only a safe default where a Redis 8 tier is not yet
    GA in the target region; chronicle requires Redis >= 7 for the {__ds}
    hash-tag keyspace and is validated on Redis 8.
  EOT
}
