#!/bin/bash

set -e

COMPOSE_FILE="docker/docker-compose.city-rover.yml"
NETWORK="city-rover-net"
CONNECT_URL="http://localhost:8083"
CONNECTOR_NAME="json-to-protobuf-telemetry"
CONNECTOR_CONFIG="json-to-protobuf-telemetry.json"

echo "=== Starting CityRover ZIO pipeline (full sequence) ==="
echo ""

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
# Helper: deploy the Kafka Connect connector (idempotent)
# ============================================================

deploy_connector() {
    echo ""
    echo "Deploying Kafka Connect connector: $CONNECTOR_NAME..."

    if [ ! -f "$CONNECTOR_CONFIG" ]; then
        echo "ERROR: Connector config not found: $CONNECTOR_CONFIG"
        echo "  Run this script from the repo root (spatio_temporal_stream_processing/)."
        exit 1
    fi

    if curl -sf "$CONNECT_URL/connectors/$CONNECTOR_NAME" >/dev/null 2>&1; then
        echo "Connector already exists: $CONNECTOR_NAME (skipping POST)"
    else
        local http_code
        http_code=$(curl -s -o /tmp/connector-create.json -w "%{http_code}" \
            -X POST "$CONNECT_URL/connectors" \
            -H 'Content-Type: application/json' \
            --data @"$CONNECTOR_CONFIG")

        if [ "$http_code" -ne 201 ]; then
            echo "ERROR: Failed to create connector (HTTP $http_code)."
            cat /tmp/connector-create.json | jq 2>/dev/null || cat /tmp/connector-create.json
            exit 1
        fi

        echo "Connector created (waiting for task to start)..."
    fi

    # Wait for RUNNING
    local max_retries=30
    local sleep_seconds=2
    local ready=false

    for i in $(seq 1 "$max_retries"); do
        local status_json
        status_json=$(curl -s "$CONNECT_URL/connectors/$CONNECTOR_NAME/status")

        local connector_state
        connector_state=$(echo "$status_json" | jq -r '.connector.state // "UNKNOWN"')
        local tasks_not_running
        tasks_not_running=$(echo "$status_json" | jq '[.tasks // [] | .[] | select(.state != "RUNNING")] | length')
        local task_count
        task_count=$(echo "$status_json" | jq '(.tasks // []) | length')

        echo "  connector=$connector_state, tasks=$task_count, not_running=$tasks_not_running (attempt $i/$max_retries)"

        if [ "$connector_state" = "RUNNING" ] && [ "$tasks_not_running" -eq 0 ] && [ "$task_count" -gt 0 ]; then
            ready=true
            break
        fi

        sleep "$sleep_seconds"
    done

    if [ "$ready" != "true" ]; then
        echo ""
        echo "ERROR: Connector did not reach RUNNING state."
        echo ""
        echo "Full connector status:"
        curl -s "$CONNECT_URL/connectors/$CONNECTOR_NAME/status" | jq
        echo ""
        echo "Task traces (if any):"
        curl -s "$CONNECT_URL/connectors/$CONNECTOR_NAME/status" \
            | jq -r '.tasks // [] | .[].trace // empty'
        echo ""
        echo "Kafka Connect logs (last 100 lines):"
        docker logs --tail 100 kafka-connect || true
        exit 1
    fi

    echo "Connector is RUNNING."
}

# ============================================================
# 0. Start PostGIS (dependency of graph-engine)
# ============================================================

echo ""
echo "0. Starting PostGIS (real_deal/analytics)..."

cd ~/Projects/real_deal/analytics
docker compose up -d postgis-real-deal
cd ~/Projects/spatio_temporal_stream_processing

echo "PostGIS started."

# ============================================================
# 1. Start infrastructure + Kafka Connect + observability
# ============================================================

echo ""
echo "1. Starting infrastructure (start-system-city-rover-zio.sh)..."
./start-system-city-rover-zio.sh

# ============================================================
# 2. Deploy the Kafka Connect connector (JSON → protobuf)
# ============================================================

echo ""
echo "2. Deploying Kafka Connect connector..."

wait_for_http "$CONNECT_URL/connectors" "Kafka Connect REST API" 60 2

deploy_connector

# ============================================================
# 3. Start the rover simulator (data generator)
# ============================================================

echo ""
echo "3. Starting rover-simulator..."
./run-rover-simulator.sh

# ============================================================
# 4. Start the ZIO pipeline (transform + writer)
# ============================================================

echo ""
echo "4. Starting ZIO pipeline (run-feature-pipeline-zio.sh)..."
./run-feature-pipeline-zio.sh

# ============================================================
# Verification: check that protobuf messages flow
# ============================================================

echo ""
echo "Waiting 15s for the simulator to produce the first records..."
sleep 15

echo ""
echo "Checking that protobuf messages are flowing into Kafka..."

PROTO_COUNT=$(docker exec -i kafka-1 \
    /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka-1:19092 \
    --topic rover-telemetry-protobuf \
    --partition 0 \
    --offset 0 \
    --max-messages 1 \
    --timeout-ms 5000 \
    2>/dev/null | wc -l || true)

if [ "$PROTO_COUNT" -gt 0 ]; then
    echo "Protobuf messages detected in rover-telemetry-protobuf (partition 0)."
else
    echo "WARNING: No protobuf messages detected yet."
    echo "  This can be normal if the simulator is still warming up."
    echo "  Check manually with:"
    echo "    docker exec -i kafka-1 /opt/kafka/bin/kafka-console-consumer.sh \\"
    echo "      --bootstrap-server kafka-1:19092 \\"
    echo "      --topic rover-telemetry-protobuf --max-messages 1 --timeout-ms 10000"
fi

# ============================================================
# Final status
# ============================================================

echo ""
echo "============================================================"
echo "=== CityRover ZIO pipeline is running ====================="
echo "============================================================"
echo ""
echo "Services:"
echo "  - PostGIS (real_deal/analytics)  : cre_db on localhost"
echo "  - kafka-1                        : localhost:19092"
echo "  - kafka-connect                  : http://localhost:8083"
echo "  - minio                          : http://localhost:9001 (console)"
echo "  - cassandra                      : localhost:9042"
echo "  - prometheus                     : http://localhost:9095"
echo "  - grafana                        : http://localhost:3000"
echo "  - feature-transform-zio          : http://localhost:9084/metrics"
echo "  - feature-writer-zio             : http://localhost:9085/metrics"
echo ""
echo "Kafka Connect:"
echo "  Connector: $CONNECTOR_NAME (RUNNING)"
echo "  Status:    $CONNECT_URL/connectors/$CONNECTOR_NAME/status"
echo ""
echo "Verify the pipeline:"
echo "  Cassandra:"
echo "    docker exec cassandra cqlsh -e 'SELECT COUNT(*) FROM city_rover.rover_features;'"
echo ""
echo "  Writer metrics:"
echo "    curl -s http://localhost:9085/metrics | grep -E 'consumed_records_total|written_records_total|write_errors_total'"
echo ""
echo "  Transform metrics:"
echo "    curl -s http://localhost:9084/metrics | grep -E 'kafka_to_zio_ms|transform_ms'"
echo ""
