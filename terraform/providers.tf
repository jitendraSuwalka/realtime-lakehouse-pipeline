# ---------------------------------------------------------------------------
# Terraform: infrastructure as CODE. Everything AWS-side that Phase 6 needs
# is declared in these .tf files. Three commands run the whole show:
#   terraform init      - download the providers (once)
#   terraform apply     - create/update everything declared here
#   terraform destroy   - delete ALL of it (the money-saving command)
#
# Terraform keeps a STATE file (terraform.tfstate) recording what it built.
# The state contains secrets (the Redshift password), which is why
# *.tfstate is gitignored - state is never committed.
# ---------------------------------------------------------------------------

terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

provider "aws" {
  region = var.region # every resource below lands in this one region
}
