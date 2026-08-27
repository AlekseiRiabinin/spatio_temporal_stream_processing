#!/bin/bash
set -e

COMPOSE_FILE="docker/docker-compose.city-rover.yml"

echo "=== Stopping rover-simulator... ==="
docker compose -f "$COMPOSE_FILE" stop rover-simulator
docker compose -f "$COMPOSE_FILE" rm -f rover-simulator

echo "=== Stopping graph-engine... ==="
docker compose -f "$COMPOSE_FILE" stop graph-engine
docker compose -f "$COMPOSE_FILE" rm -f graph-engine

echo "=== Done. Both services stopped and removed. ==="
