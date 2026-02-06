variable "project_id" {
  type        = string
  description = "The ID of the project to create the resources in"
  default     = "de-2026-workshop"
}

variable "region" {
  type        = string
  description = "The region to create the resources in"
  default     = "us-central1"
}

variable "location" {
  type        = string
  description = "Project location"
  default     = "US"
}

variable "gcs_bucket_name" {
  type        = string
  description = "My GCS bucket name"
  default     = "demo_bucket"
}

variable "gcs_storage_class" {
  type        = string
  description = "The storage class to use for the bucket"
  default     = "STANDARD"
}
variable "gcs_force_destroy" {
  type        = bool
  description = "Whether to force destroy the GCS bucket"
  default     = true
}

variable "bq_dataset_name" {
  type        = string
  description = "My BigQuery dataset name"
  default     = "demo_dataset"
}