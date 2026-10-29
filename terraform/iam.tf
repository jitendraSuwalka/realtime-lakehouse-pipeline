# ---------------------------------------------------------------------------
# The role Redshift assumes when IT reads S3 (the COPY command).
# This is the idiomatic AWS pattern: the warehouse is GRANTED read access
# to the lake - no access keys ever appear inside SQL.
# ---------------------------------------------------------------------------

resource "aws_iam_role" "redshift_copy" {
  name = "${var.project}-redshift-copy"

  # WHO may wear this role: the Redshift service itself.
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "redshift.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# WHAT the role may do: read our one bucket. Least privilege - it cannot
# write to S3, and it cannot see any other bucket.
resource "aws_iam_role_policy" "redshift_copy_s3_read" {
  name = "s3-read-lakehouse"
  role = aws_iam_role.redshift_copy.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:ListBucket"]
      Resource = [
        aws_s3_bucket.lakehouse.arn,
        "${aws_s3_bucket.lakehouse.arn}/*",
      ]
    }]
  })
}
