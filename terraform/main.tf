# Create GCS bucket
resource "google_storage_bucket" "tf_bucket" {
  name          = var.gcs_bucket_name
  location      = var.region
  force_destroy = true
}

# Create BigQuery dataset
resource "google_bigquery_dataset" "tf_dataset" {
  dataset_id = var.dataset_id
  location   = var.region
}

# Create VM
resource "google_compute_instance" "tf_vm" {
  name         = var.vm_name
  machine_type = "e2-micro"
  zone         = var.zone

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-11"
    }
  }

  network_interface {
    network       = "default"
    access_config {}
  }
}
