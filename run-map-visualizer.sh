#!/bin/bash
set -e

COMPOSE_FILE="docker/docker-compose.city-rover.yml"

echo "=== Starting rover-map-visualizer... ==="

docker compose -f "$COMPOSE_FILE" up -d rover-map-visualizer
