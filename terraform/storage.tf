resource "google_storage_bucket" "gcp-project-bucket" {
  name          = "de-project-gcp-bucket"
  location      = var.region
  force_destroy = true
}