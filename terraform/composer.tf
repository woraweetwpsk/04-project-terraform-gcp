resource "google_composer_environment" "gcp-project-composer" {
  name   = "project-gcp-composer"
  region = var.region
  config {

    software_config {
      image_version = "composer-3-airflow-2"
      pypi_packages = {
        "requests" = ""
      }
    }

    workloads_config {
      scheduler {
        cpu        = 0.5
        memory_gb  = 2
        storage_gb = 1
        count      = 1
      }
      triggerer {
        cpu        = 0.5
        memory_gb  = 1
        count      = 1
      }
      web_server {
        cpu        = 0.5
        memory_gb  = 2
        storage_gb = 1
      }
      worker {
        cpu = 1
        memory_gb  = 4
        storage_gb = 10
        min_count  = 1
        max_count  = 3
      }

    }
    environment_size = "ENVIRONMENT_SIZE_SMALL"
  }
}
