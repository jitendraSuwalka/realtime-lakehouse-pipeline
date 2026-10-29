variable "region" {
  description = "AWS region for all resources"
  type        = string
  default     = "eu-west-1" # Ireland
}

variable "project" {
  description = "Name prefix for every resource (also used for tagging)"
  type        = string
  default     = "realtime-lakehouse"
}

variable "my_ip" {
  description = <<-EOT
    YOUR current public IP in CIDR form, e.g. "203.0.113.7/32".
    Only this address may reach the Redshift endpoint (port 5439).
    Find it with:  curl -s ifconfig.me
    Set it in terraform.tfvars (gitignored - never committed).
  EOT
  type        = string
}
