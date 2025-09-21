terraform {
  backend "gcs" {
    bucket = "terraform_stata"
    prefix = "terraform/state"
  }
}
