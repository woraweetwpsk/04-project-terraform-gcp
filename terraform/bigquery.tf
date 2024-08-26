resource "google_bigquery_dataset" "gcp-project-bigquery" {
  dataset_id = "project_gcp_dataset"
  location   = var.region
}
