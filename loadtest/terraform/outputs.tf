output "cluster_name" {
  value = google_container_cluster.this.name
}

output "kubeconfig_command" {
  description = "Run this to point kubectl at the cluster."
  value       = "gcloud container clusters get-credentials ${google_container_cluster.this.name} --region ${var.region} --project ${var.project_id}"
}

output "redis_url" {
  description = "Feed this into the experiment spec's sut.redis_url (empty when provision_redis=false)."
  value = var.provision_redis ? format(
    "redis://%s:%d/0",
    google_redis_instance.this[0].host,
    google_redis_instance.this[0].port,
  ) : ""
}
