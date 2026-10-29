# ---------------------------------------------------------------------------
# The REAL lake bucket. Same layout as LocalStack used locally:
#   bronze/payments/ingest_date=.../   silver/payments/event_date=.../
#
# Bucket names are globally unique across ALL AWS customers, so a random
# suffix guarantees ours never collides with anyone else's.
# ---------------------------------------------------------------------------

resource "random_id" "bucket_suffix" {
  byte_length = 4 # 8 hex characters
}

resource "aws_s3_bucket" "lakehouse" {
  bucket = "${var.project}-${random_id.bucket_suffix.hex}"

  # force_destroy lets `terraform destroy` delete the bucket even when it
  # still holds data. Right for a teardown-daily learning project;
  # in production you would NEVER set this on a data lake.
  force_destroy = true

  tags = {
    Project = var.project
    Managed = "terraform"
  }
}

# No public access, ever. Four switches, all on.
resource "aws_s3_bucket_public_access_block" "lakehouse" {
  bucket                  = aws_s3_bucket.lakehouse.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
