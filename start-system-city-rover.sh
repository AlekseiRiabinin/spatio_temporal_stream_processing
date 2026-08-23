#!/bin/bash

set -e

COMPOSE_FILE="docker/docker-compose.city-rover.yml"

echo "=== Starting CityRover System ==="
echo "Compose file: $COMPOSE_FILE"
echo ""


# ------------------------------------------------------------
# 1. Start Kafka
# ------------------------------------------------------------

echo "1. Starting Kafka..."

docker compose -f "$COMPOSE_FILE" up -d kafka-1

echo ""
echo "Waiting for Kafka..."

for i in {1..20}; do

    if docker exec kafka-1 bash -c \
        "/opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server kafka-1:19092 \
        --list" \
        >/dev/null 2>&1; then

        echo "Kafka is ready."
        break
    fi

    echo "Kafka not ready yet... retrying ($i/20)"

    sleep 2

    if [ "$i" -eq 20 ]; then
        echo "ERROR: Kafka did not become ready."
        exit 1
    fi
done


# ------------------------------------------------------------
# 2. Create Kafka topics
# ------------------------------------------------------------

echo ""
echo "2. Creating Kafka topics..."

docker exec kafka-1 bash -c '
    topics=(
        "rover-telemetry-raw:4"
        "rover-telemetry-enriched:4"
        "rover-analytics:4"
        "cityrover-spark-metrics:1"
    )

    for topic in "${topics[@]}"; do

        IFS=":" read -r name partitions <<< "$topic"

        if /opt/kafka/bin/kafka-topics.sh \
            --bootstrap-server kafka-1:19092 \
            --describe \
            --topic "$name" \
            >/dev/null 2>&1; then

            echo "Topic exists: $name"

        else

            echo "Creating topic: $name"

            /opt/kafka/bin/kafka-topics.sh \
                --create \
                --topic "$name" \
                --partitions "$partitions" \
                --replication-factor 1 \
                --bootstrap-server kafka-1:19092
        fi
    done
'

echo "Kafka topics initialized."


# ------------------------------------------------------------
# 3. Start MinIO + initialize buckets
# ------------------------------------------------------------

echo ""
echo "3. Starting MinIO..."

docker compose -f "$COMPOSE_FILE" up -d minio

echo ""
echo "Initializing MinIO..."

docker compose -f "$COMPOSE_FILE" up minio-setup

echo "MinIO initialized."


# ------------------------------------------------------------
# 4. Start PostgreSQL
# ------------------------------------------------------------

echo ""
echo "4. Starting PostgreSQL..."

docker compose -f "$COMPOSE_FILE" up -d postgres

echo ""
echo "Waiting for PostgreSQL..."

for i in {1..30}; do

    if docker exec postgres \
        pg_isready \
        -U postgres \
        >/dev/null 2>&1; then

        echo "PostgreSQL is ready."
        break
    fi

    echo "PostgreSQL not ready yet... retrying ($i/30)"

    sleep 2

    if [ "$i" -eq 30 ]; then
        echo "ERROR: PostgreSQL did not become ready."
        exit 1
    fi
done


# ------------------------------------------------------------
# 5. Start Hive Metastore
# ------------------------------------------------------------

echo ""
echo "5. Starting Hive Metastore..."

docker compose -f "$COMPOSE_FILE" up -d hive-metastore

echo ""
echo "Waiting for Hive Metastore..."

for i in {1..30}; do

    if docker exec hive-metastore \
        nc -z localhost 9083 \
        >/dev/null 2>&1; then

        echo "Hive Metastore is ready."
        break
    fi

    echo "Hive Metastore not ready yet... retrying ($i/30)"

    sleep 3

    if [ "$i" -eq 30 ]; then
        echo "ERROR: Hive Metastore did not become ready."
        echo ""
        echo "Hive Metastore logs:"
        docker logs --tail 100 hive-metastore
        exit 1
    fi
done


# ------------------------------------------------------------
# 6. Start Trino
# ------------------------------------------------------------

echo ""
echo "6. Starting Trino..."

docker compose -f "$COMPOSE_FILE" up -d trino

echo ""
echo "Waiting for Trino..."

for i in {1..30}; do

    if curl -sf \
        http://localhost:8090/v1/info \
        >/dev/null 2>&1; then

        echo "Trino is ready."
        break
    fi

    echo "Trino not ready yet... retrying ($i/30)"

    sleep 3

    if [ "$i" -eq 30 ]; then
        echo "ERROR: Trino did not become ready."
        echo ""
        echo "Trino logs:"
        docker logs --tail 100 trino
        exit 1
    fi
done


# ------------------------------------------------------------
# 7. Start graph engine
# ------------------------------------------------------------

echo ""
echo "7. Starting graph-engine..."

docker compose -f "$COMPOSE_FILE" up -d graph-engine


# ------------------------------------------------------------
# 8. Start rover simulator
# ------------------------------------------------------------

echo ""
echo "8. Starting rover-simulator..."

docker compose -f "$COMPOSE_FILE" up -d rover-simulator


# ------------------------------------------------------------
# Done
# ------------------------------------------------------------

echo ""
echo "=== CityRover System is running ==="
echo ""

echo "Services:"
echo "  - kafka-1"
echo "  - minio"
echo "  - postgres"
echo "  - hive-metastore"
echo "  - trino"
echo "  - graph-engine"
echo "  - rover-simulator"

echo ""

echo "MinIO Console:"
echo "  http://localhost:9001"

echo ""
echo "MinIO S3 API:"
echo "  http://localhost:9002"

echo ""
echo "Hive Metastore:"
echo "  thrift://localhost:9096"

echo ""
echo "Trino UI:"
echo "  http://localhost:8090"

echo ""
echo "Kafka:"
echo "  kafka-1:19092"

echo ""
