#!/bin/bash
set -e

COMPOSE_FILE="docker/docker-compose.city-rover.yml"

echo "=== Starting graph-engine... ==="

docker compose -f "$COMPOSE_FILE" up -d graph-engine

echo "=== Starting rover-simulator... ==="

docker compose -f "$COMPOSE_FILE" up -d rover-simulator
