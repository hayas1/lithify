#!/bin/sh
set -eu

export AWS_ACCESS_KEY_ID="${S3_ACCESS_KEY_ID}"
export AWS_SECRET_ACCESS_KEY="${S3_SECRET_ACCESS_KEY}"
export AWS_DEFAULT_REGION="${S3_REGION}"

if ! aws --endpoint-url "${S3_ENDPOINT}" s3api head-bucket --bucket "${S3_BUCKET}" 2>/dev/null; then
    aws --endpoint-url "${S3_ENDPOINT}" s3api create-bucket --bucket "${S3_BUCKET}"
fi
