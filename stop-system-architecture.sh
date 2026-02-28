#!/bin/bash

echo "Stopping all services..."
docker compose -f docker/docker-compose.architecture.yml down
echo "✅ All services stopped!"
