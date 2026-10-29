# What the pipeline scripts need to know, straight from Terraform:
#   terraform output                        - show all (secrets hidden)
#   terraform output -raw bucket_name       - one value, pipe-able
#   terraform output -raw redshift_admin_password   - the secret, on demand

output "bucket_name" {
  description = "The real S3 lake bucket"
  value       = aws_s3_bucket.lakehouse.bucket
}

output "redshift_endpoint" {
  description = "Redshift Serverless host for dbt/loader connections"
  value       = aws_redshiftserverless_workgroup.lakehouse.endpoint[0].address
}

output "redshift_port" {
  value = aws_redshiftserverless_workgroup.lakehouse.endpoint[0].port
}

output "redshift_database" {
  value = aws_redshiftserverless_namespace.lakehouse.db_name
}

output "redshift_admin_user" {
  value = aws_redshiftserverless_namespace.lakehouse.admin_username
}

output "redshift_admin_password" {
  value     = random_password.redshift_admin.result
  sensitive = true # hidden from normal output; -raw reveals it deliberately
}
