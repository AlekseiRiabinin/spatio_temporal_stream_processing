#!/bin/bash
set -e

COMPOSE_FILE="docker/docker-compose.city-rover.yml"

# ============================================================
# Start feature-transform-zio
# ============================================================

echo ""
echo "Starting feature-transform-zio..."

docker compose \
    -f "$COMPOSE_FILE" \
    up -d feature-transform-zio

verify_network "feature-transform-zio"

wait_for_http \
    "http://localhost:9084/metrics" \
    "feature-transform-zio /metrics" \
    60 \
    2

# ============================================================
# Start feature-writer-zio
# ============================================================

echo ""
echo "Starting feature-writer-zio..."

docker compose \
    -f "$COMPOSE_FILE" \
    up -d feature-writer-zio

verify_network "feature-writer-zio"

wait_for_http \
    "http://localhost:9085/metrics" \
    "feature-writer-zio /metrics" \
    60 \
    2
