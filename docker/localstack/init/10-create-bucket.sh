#!/bin/bash
set -e
# LocalStack "ready" hook: create the S3 bucket that backs the lakehouse.
awslocal s3api create-bucket --bucket lakehouse >/dev/null 2>&1 || true
awslocal s3api put-bucket-versioning --bucket lakehouse \
  --versioning-configuration Status=Enabled >/dev/null 2>&1 || true
awslocal s3 ls
