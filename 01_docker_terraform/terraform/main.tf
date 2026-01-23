resource "google_storage_bucket" "data_lake_bucket" {
  name          = var.bucket_name
  location      = var.region
  storage_class = "STANDARD"

  uniform_bucket_level_access = true

  force_destroy = true
}