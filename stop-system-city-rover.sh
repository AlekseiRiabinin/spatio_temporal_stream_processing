#!/bin/bash

set -e

COMPOSE_FILE="docker/docker-compose.city-rover.yml"

echo "=== Stopping CityRover System ==="

docker compose -f "$COMPOSE_FILE" down

echo ""
echo "=== CityRover System stopped ==="
