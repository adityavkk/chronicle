# Ephemeral GKE cluster for the load test: apply -> run experiment -> destroy.
# deletion_protection is off so `terraform destroy` always succeeds.
resource "google_container_cluster" "this" {
  name     = var.cluster_name
  location = var.region
  network  = var.network

  remove_default_node_pool = true
  initial_node_count       = 1
  deletion_protection      = false
}

locals {
  pools = {
    sut     = { machine = var.sut_machine_type, count = var.sut_node_count }
    loadgen = { machine = var.loadgen_machine_type, count = var.loadgen_node_count }
    obs     = { machine = var.obs_machine_type, count = var.obs_node_count }
  }
}

resource "google_container_node_pool" "pools" {
  for_each = local.pools

  name       = each.key
  location   = var.region
  cluster    = google_container_cluster.this.name
  node_count = each.value.count

  node_config {
    machine_type = each.value.machine
    # role=<pool> lets a workload pin itself with nodeSelector { role: sut }.
    labels       = { role = each.key }
    oauth_scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }
}
