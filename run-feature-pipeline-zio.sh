#!/bin/bash

set -e

COMPOSE_FILE="docker/docker-compose.city-rover.yml"
NETWORK="city-rover-net"

# ============================================================
# Helper: wait for an HTTP endpoint to become ready
# ============================================================

wait_for_http() {
    local url="$1"
    local description="$2"
    local retries="${3:-60}"
    local sleep_seconds="${4:-2}"

    echo ""
    echo "Waiting for $description..."

    for i in $(seq 1 "$retries"); do
        if curl -sf "$url" >/dev/null 2>&1; then
            echo "$description is ready."
            return 0
        fi

        echo "$description not ready yet... retrying ($i/$retries)"
        sleep "$sleep_seconds"
    done

    echo "ERROR: $description did not become ready."
    return 1
}

# ============================================================
# Helper: verify container is attached to city-rover-net
# ============================================================

verify_network() {
    local container="$1"

    if ! docker inspect "$container" \
        --format '{{json .NetworkSettings.Networks}}' \
        | grep -q "\"$NETWORK\""; then

        echo ""
        echo "ERROR: Container '$container' is not attached to '$NETWORK'."
        echo ""
        docker inspect "$container" \
            --format '{{range $name, $network := .NetworkSettings.Networks}}{{$name}}{{"\n"}}{{end}}' \
            || true
        echo ""
        exit 1
    fi
}

# ============================================================
# Start feature-transform-zio
# ============================================================

echo ""
echo "Starting feature-transform-zio..."

docker compose \
    -f "$COMPOSE_FILE" \
    up -d feature-transform-zio

verify_network "feature-transform-zio"

wait_for_http \
    "http://localhost:9084/metrics" \
    "feature-transform-zio /metrics" \
    60 \
    2

# ============================================================
# Start feature-writer-zio
# ============================================================

echo ""
echo "Starting feature-writer-zio..."

docker compose \
    -f "$COMPOSE_FILE" \
    up -d feature-writer-zio

verify_network "feature-writer-zio"

wait_for_http \
    "http://localhost:9085/metrics" \
    "feature-writer-zio /metrics" \
    60 \
    2
