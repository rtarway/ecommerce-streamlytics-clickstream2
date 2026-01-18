#!/bin/bash

# Set PubSub Emulator Host
export PUBSUB_EMULATOR_HOST=localhost:8085
# Use PROJECT_ID from environment or default to a dummy value
export PROJECT_ID=${PROJECT_ID:-test-project}

echo "Creating Topics..."
gcloud pubsub topics create clickstream --project=$PROJECT_ID
gcloud pubsub topics create inventory --project=$PROJECT_ID
gcloud pubsub topics create transactions --project=$PROJECT_ID

echo "Creating Subscriptions..."
gcloud pubsub subscriptions create clickstream-sub --topic=clickstream --project=$PROJECT_ID
gcloud pubsub subscriptions create inventory-sub --topic=inventory --project=$PROJECT_ID
gcloud pubsub subscriptions create transactions-sub --topic=transactions --project=$PROJECT_ID

echo "PubSub Emulator Setup Complete."
