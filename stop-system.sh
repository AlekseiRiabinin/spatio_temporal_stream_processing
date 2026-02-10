#!/bin/bash

echo "Stopping all services..."
docker compose -f docker/docker-compose.yml down
echo "✅ All services stopped!"
