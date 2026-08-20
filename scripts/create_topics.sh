#!/usr/bin/env bash
set -euo pipefail
BROKER_CONTAINER="${BROKER_CONTAINER:-kyc-redpanda}"
TOPICS=("kyc.events.v1" "kyc.events.dlq.v1")
for topic in "${TOPICS[@]}"; do
  docker exec "$BROKER_CONTAINER" rpk topic create "$topic" --partitions 3 --replicas 1 || true
done
docker exec "$BROKER_CONTAINER" rpk topic list
