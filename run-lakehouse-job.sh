#!/bin/bash
set -e

COMPOSE_FILE="docker/docker-compose.city-rover.yml"

echo "=== Starting trajectory lakehouse writer... ==="

docker compose -f "$COMPOSE_FILE" up -d trajectory-lakehouse-writer-job
