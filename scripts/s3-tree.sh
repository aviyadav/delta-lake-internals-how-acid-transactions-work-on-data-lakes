#!/bin/sh
# Show the physical lakehouse layout through the S3 API (no Delta semantics).
#   ./scripts/s3-tree.sh              bucket overview
#   ./scripts/s3-tree.sh bronze/orders   one table, including _delta_log
set -e
PREFIX="${1:-}"
docker compose exec -T localstack awslocal s3 ls "s3://lakehouse/${PREFIX}" --recursive --human-readable
