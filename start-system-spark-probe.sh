#!/bin/bash
set -e

COMPOSE_FILE="docker/docker-compose.fs-stream.yml"
NETWORK="city-rover-net"

# ------------------------------------------------------------
# Spark FS cluster container names & endpoints
# ------------------------------------------------------------
SPARK_MASTER_CONTAINER="spark-master-fs"
SPARK_WORKER_CONTAINER="spark-worker-fs"
SPARK_MASTER_HOST="spark-master-fs"
SPARK_MASTER_RPC_INTERNAL="spark://spark-master-fs:7077"
SPARK_MASTER_RPC_EXTERNAL="localhost:7078"
SPARK_MASTER_UI_EXTERNAL="http://localhost:8090"
SPARK_WORKER_UI_EXTERNAL="http://localhost:8091"

echo "=== Starting Spark Probe System (Kafka + Cassandra + Spark) ==="
echo "Compose file: $COMPOSE_FILE"
echo "Docker network: $NETWORK"
echo ""

# ============================================================
# Helpers
# ============================================================
wait_for_container() {
    local container="$1"
    local command="$2"
    local description="$3"
    local retries="${4:-30}"
    local sleep_seconds="${5:-2}"

    echo ""
    echo "Waiting for $description..."

    for i in $(seq 1 "$retries"); do
        if docker exec "$container" sh -c "$command" >/dev/null 2>&1; then
            echo "$description is ready."
            return 0
        fi
        echo "$description not ready yet... retrying ($i/$retries)"
        sleep "$sleep_seconds"
    done

    echo ""
    echo "ERROR: $description did not become ready."
    echo "--- Last 100 log lines from $container ---"
    docker logs --tail 100 "$container" || true
    exit 1
}

ensure_network() {
    echo "Checking Docker network: $NETWORK"
    if docker network inspect "$NETWORK" >/dev/null 2>&1; then
        echo "Docker network already exists: $NETWORK"
    else
        echo "Creating Docker network: $NETWORK"
        docker network create "$NETWORK"
    fi
}

verify_network() {
    local container="$1"
    if ! docker inspect "$container" \
        --format '{{json .NetworkSettings.Networks}}' \
        | grep -q "\"$NETWORK\""; then
        echo ""
        echo "ERROR: Container '$container' is not attached to '$NETWORK'."
        docker inspect "$container" \
            --format '{{range $name, $network := .NetworkSettings.Networks}}{{$name}}{{"\n"}}{{end}}' || true
        exit 1
    fi
}

# ============================================================
# 0. Docker check + network
# ============================================================
echo "0. Checking Docker..."
if ! docker info >/dev/null 2>&1; then
    echo "ERROR: Docker is not running."
    exit 1
fi
echo "Docker is running."

echo ""
ensure_network

echo ""
echo "Validating Docker Compose configuration..."
docker compose -f "$COMPOSE_FILE" config >/dev/null
echo "Compose configuration is valid."

# ============================================================
# 1. Kafka
# ============================================================
echo ""
echo "1. Starting Kafka..."
docker compose -f "$COMPOSE_FILE" up -d kafka-1
verify_network "kafka-1"

wait_for_container \
    "kafka-1" \
    "/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --list" \
    "Kafka" \
    30 \
    2

# ============================================================
# 2. Kafka topics (recreate on every run for a clean probe state)
# ============================================================
echo ""
echo "2. Recreating Kafka topics for the Spark probe..."

# Topics are deleted and recreated on every run so the partition count
# and timestamp policy are always exactly what this script specifies.
# This wipes any existing data -- acceptable for a latency probe that
# always feeds fresh records from probe_producer_exp1.py.
docker exec kafka-1 bash -c '
    topics=(
        "stream_fs.test.intellinx_antifraud_dbo_fin_transactions:1:LogAppendTime"
        "stream_fs.test.intellinx_antifraud_dbo_nofin_transactions:1:LogAppendTime"
        "stream_fs.test.intellinx_antifraud_dbo_incoming_payments:1:LogAppendTime"
        "stream_fs.test.probe_transformation:1:CreateTime"
    )

    for topic in "${topics[@]}"; do
        IFS=":" read -r name partitions ts_type <<< "$topic"

        # 1. Delete the topic if it exists.
        if /opt/kafka/bin/kafka-topics.sh \
            --bootstrap-server kafka-1:19092 \
            --describe --topic "$name" >/dev/null 2>&1; then
            echo "Deleting existing topic: $name"
            /opt/kafka/bin/kafka-topics.sh \
                --bootstrap-server kafka-1:19092 \
                --delete --topic "$name"
        else
            echo "Topic does not exist yet: $name"
        fi
    done
'
echo "Topic deletion issued; waiting for Kafka to finish removing them..."

# Kafka deletes topics asynchronously. Poll until all four names are gone
# from --list, otherwise the subsequent --create may race with the delete
# and fail with TopicExistsException.
for i in $(seq 1 30); do
    REMAINING=$(docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server kafka-1:19092 --list 2>/dev/null \
        | grep -cE "stream_fs\.test\.(intellinx_antifraud_dbo_(fin|nofin|incoming)_transactions|intellinx_antifraud_dbo_incoming_payments|probe_transformation)" || true)
    if [ "$REMAINING" -eq 0 ]; then
        echo "All probe topics deleted."
        break
    fi
    echo "Waiting for topic deletion... ($i/30, $REMAINING remaining)"
    sleep 1
    if [ "$i" -eq 30 ]; then
        echo "ERROR: topics did not disappear after 30 seconds."
        docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
            --bootstrap-server kafka-1:19092 --list || true
        exit 1
    fi
done

# 2. Recreate them with the intended partition count and timestamp policy.
docker exec kafka-1 bash -c '
    topics=(
        "stream_fs.test.intellinx_antifraud_dbo_fin_transactions:1:LogAppendTime"
        "stream_fs.test.intellinx_antifraud_dbo_nofin_transactions:1:LogAppendTime"
        "stream_fs.test.intellinx_antifraud_dbo_incoming_payments:1:LogAppendTime"
        "stream_fs.test.probe_transformation:1:CreateTime"
    )

    for topic in "${topics[@]}"; do
        IFS=":" read -r name partitions ts_type <<< "$topic"

        echo "Creating topic: $name"
        /opt/kafka/bin/kafka-topics.sh \
            --create --topic "$name" \
            --partitions "$partitions" \
            --replication-factor 1 \
            --bootstrap-server kafka-1:19092

        if [ "$ts_type" = "LogAppendTime" ]; then
            /opt/kafka/bin/kafka-configs.sh \
                --bootstrap-server kafka-1:19092 \
                --alter \
                --entity-type topics \
                --entity-name "$name" \
                --add-config message.timestamp.type=LogAppendTime >/dev/null
        else
            /opt/kafka/bin/kafka-configs.sh \
                --bootstrap-server kafka-1:19092 \
                --alter \
                --entity-type topics \
                --entity-name "$name" \
                --delete-config message.timestamp.type >/dev/null 2>&1 || true
        fi
    done
'
echo "Kafka topics recreated."

# ============================================================
# 3. Cassandra
# ============================================================
echo ""
echo "3. Starting Cassandra..."
docker compose -f "$COMPOSE_FILE" up -d cassandra
verify_network "cassandra"

wait_for_container \
    "cassandra" \
    "cqlsh cassandra 9042 -e 'DESCRIBE KEYSPACES'" \
    "Cassandra" \
    20 \
    10
echo "Cassandra is ready."

# ============================================================
# 4. Spark Master
# ============================================================
echo ""
echo "4. Starting Spark Master..."
docker compose -f "$COMPOSE_FILE" up -d "$SPARK_MASTER_CONTAINER"
verify_network "$SPARK_MASTER_CONTAINER"

wait_for_container \
    "$SPARK_MASTER_CONTAINER" \
    "bash -c 'exec 3<>/dev/tcp/spark-master-fs/7077' && echo ok" \
    "Spark Master (RPC port 7077)" \
    30 \
    2

wait_for_container \
    "$SPARK_MASTER_CONTAINER" \
    "bash -c 'exec 3<>/dev/tcp/spark-master-fs/8080' && echo ok" \
    "Spark Master (Web UI port 8080)" \
    30 \
    2

# ============================================================
# 5. Spark Worker
# ============================================================
echo ""
echo "5. Starting Spark Worker..."
docker compose -f "$COMPOSE_FILE" up -d "$SPARK_WORKER_CONTAINER"
verify_network "$SPARK_WORKER_CONTAINER"

SPARK_WORKER_READY=0
for i in $(seq 1 30); do
    if docker exec "$SPARK_MASTER_CONTAINER" bash -c \
        "exec 3<>/dev/tcp/spark-master-fs/8080 && printf 'GET /json/ HTTP/1.0\r\n\r\n' >&3 && cat <&3" \
        2>/dev/null | grep -q '"aliveworkers" *: *[1-9]'; then
        SPARK_WORKER_READY=1
        break
    fi
    echo "Spark Worker not registered yet... retrying ($i/30)"
    sleep 2
done

if [ "$SPARK_WORKER_READY" -ne 1 ]; then
    echo "ERROR: Spark Worker did not register with the master."
    echo "--- Last 100 log lines from $SPARK_WORKER_CONTAINER ---"
    docker logs --tail 100 "$SPARK_WORKER_CONTAINER" || true
    echo "--- Last 100 log lines from $SPARK_MASTER_CONTAINER ---"
    docker logs --tail 100 "$SPARK_MASTER_CONTAINER" || true
    exit 1
fi
echo "Spark Worker is registered with the master."

# ============================================================
# 6. Verify Kafka connector JARs inside the Spark worker
# ============================================================
echo ""
echo "6. Verifying Kafka connector JARs in Spark worker..."
SPARK_KAFKA_JARS=$(docker exec "$SPARK_WORKER_CONTAINER" sh -c \
    'ls /opt/spark/jars/ 2>/dev/null | grep -E "spark-sql-kafka|kafka-clients" || true')
if [ -n "$SPARK_KAFKA_JARS" ]; then
    echo "Kafka connector JAR(s) found in $SPARK_WORKER_CONTAINER:"
    echo "$SPARK_KAFKA_JARS" | sed 's/^/  - /'
else
    echo "WARNING: No Kafka connector JARs found in /opt/spark/jars/ of $SPARK_WORKER_CONTAINER."
    echo "Check the volumes section of $SPARK_WORKER_CONTAINER in $COMPOSE_FILE:"
    echo "  ../spark/jars/spark-sql-kafka-0-10_2.13-4.3.0.jar"
    echo "  ../spark/jars/kafka-clients-3.8.0.jar"
    echo "  ../spark/jars/spark-token-provider-kafka-0-10_2.13-4.3.0.jar"
fi

# ============================================================
# Final network verification
# ============================================================
echo ""
echo "Checking Docker network: $NETWORK"
echo ""
echo "Containers attached to $NETWORK:"
docker network inspect "$NETWORK" \
    --format '{{range $id, $container := .Containers}}  - {{$container.Name}}{{"\n"}}{{end}}'

# ============================================================
# Final status
# ============================================================
echo ""
echo "============================================================"
echo "=== Spark Probe System is running =========================="
echo "============================================================"
echo ""
echo "Services:"
echo "  - kafka-1"
echo "  - cassandra"
echo "  - $SPARK_MASTER_CONTAINER"
echo "  - $SPARK_WORKER_CONTAINER"
echo ""
echo "Endpoints:"
echo "  Kafka:                 localhost:19092"
echo "  Cassandra CQL:         localhost:9042"
echo "  Spark Master UI:       $SPARK_MASTER_UI_EXTERNAL"
echo "  Spark Worker UI:       $SPARK_WORKER_UI_EXTERNAL"
echo "  Spark Master RPC:      $SPARK_MASTER_RPC_EXTERNAL"
echo ""
echo "Next step: start the probe with"
echo "  docker compose -f $COMPOSE_FILE up -d spark_kafka_latency_probe"
echo ""
