#!/bin/bash

echo "Stopping all services..."
docker compose -f docker/docker-compose.stream-models.yml down
echo "✅ All services stopped!"
