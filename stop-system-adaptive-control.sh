#!/bin/bash

echo "Stopping all services..."
docker compose -f docker/docker-compose.adaptive-control.yml down
echo "✅ All services stopped!"
