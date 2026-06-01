#!/bin/bash
# Run this in Cloud Shell any time you update the app code.
# Usage: ./deploy.sh

set -e

PROJECT_ID="isocongestion"
BUCKET="congestion-lmp-data"
REGION="us-central1"
SERVICE="lmp-app"
IMAGE="gcr.io/${PROJECT_ID}/${SERVICE}"

echo "=== Building container image ==="
gcloud builds submit --tag "$IMAGE"

echo ""
echo "=== Deploying to Cloud Run ==="
gcloud run deploy "$SERVICE" \
  --image "$IMAGE" \
  --platform managed \
  --region "$REGION" \
  --allow-unauthenticated \
  --memory 2Gi \
  --cpu 1 \
  --timeout 300 \
  --set-env-vars "GCS_BUCKET=${BUCKET}"

echo ""
echo "=== Done! ==="
gcloud run services describe "$SERVICE" --region "$REGION" --format "value(status.url)"
