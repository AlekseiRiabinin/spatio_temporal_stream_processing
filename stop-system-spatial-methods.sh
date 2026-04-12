#!/bin/bash

echo "Stopping all services..."
docker compose -f docker/docker-compose.spatial-methods.yml down
echo "✅ All services stopped!"
