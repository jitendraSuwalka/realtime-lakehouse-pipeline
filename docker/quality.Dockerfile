# ---------------------------------------------------------------------------
# The data-quality runner: Great Expectations + the S3 readers.
# Same story as docker/dbt.Dockerfile: our laptop Python (3.14) is newer
# than GX supports, so the container brings Python 3.12 - and in production
# quality checks run containerised inside the orchestrator anyway.
#
# Build:  docker compose build quality
# Run:    docker compose run --rm quality --layer bronze
#         docker compose run --rm quality --layer silver
# ---------------------------------------------------------------------------
FROM python:3.12-slim

RUN pip install --no-cache-dir \
    "great_expectations>=1.2,<2" \
    "pandas>=2.2" \
    "pyarrow>=17.0" \
    "boto3>=1.34"

# The script is mounted at runtime (see docker-compose.yml) so editing it
# never needs a rebuild. ENTRYPOINT means extra args are passed straight
# to the script: `docker compose run --rm quality --layer bronze`.
ENTRYPOINT ["python", "/opt/quality/run_quality_checks.py"]
