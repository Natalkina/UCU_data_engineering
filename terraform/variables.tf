variable "project_id" {
  type = string
}

variable "region" {
  default = "us-central1"
}

variable "zone" {
  default = "us-central1-a"
}

variable "gcs_bucket_name" {
  default = "my-sample-tf-bucket-12345"
}

variable "dataset_id" {
  default = "my_tf_dataset"
}

variable "vm_name" {
  default = "my-tf-vm"
}
