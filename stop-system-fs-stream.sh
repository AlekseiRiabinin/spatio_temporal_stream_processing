#!/bin/bash

set -e

echo "=== Stopping FS-Stream System ==="

docker compose -f docker/docker-compose.city-rover.yml down

echo ""
echo "=== Stopping FS-Stream System stopped ==="
