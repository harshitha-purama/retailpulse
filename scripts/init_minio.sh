#!/bin/bash
set -e

echo "Waiting for MinIO..."
sleep 10

mc alias set local http://minio:9000 minioadmin minioadmin123

for BUCKET in bronze mlflow-artifacts; do
  mc mb --ignore-existing local/$BUCKET
  echo "Bucket '$BUCKET' ready."
done

mc anonymous set download local/mlflow-artifacts
echo "MinIO initialised."
