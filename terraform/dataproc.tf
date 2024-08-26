resource "google_dataproc_cluster" "gcp-project-dataproc" {
  name   = "gcp-project-dataproc-cluster"
  region = var.region2

  cluster_config {
    master_config {
      num_instances = 1
      machine_type  = "n2-standard-2"
      disk_config {
        boot_disk_type    = "pd-ssd"
        boot_disk_size_gb = 100
      }
    }

    worker_config {
      num_instances = 0
    }
  }
}