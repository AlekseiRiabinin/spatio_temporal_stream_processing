#!/bin/bash
set -e

COMPOSE="docker/docker-compose.spatial-methods.yml"

TIMESTAMP_PATTERN_ARG=${1:-}
EVENT_RATE_PATTERN_ARG=${2:-}
GEOMETRY_PATTERN_ARG=${3:-}
MOTION_MODE_ARG=${4:-}

echo "=== Starting Spatial Methods System (Article 03) ==="
[ -n "$TIMESTAMP_PATTERN_ARG" ] && echo "Timestamp Pattern:   $TIMESTAMP_PATTERN_ARG"
[ -n "$EVENT_RATE_PATTERN_ARG" ] && echo "Rate Pattern:        $EVENT_RATE_PATTERN_ARG"
[ -n "$GEOMETRY_PATTERN_ARG" ] && echo "Geometry Pattern:    $GEOMETRY_PATTERN_ARG"
[ -n "$MOTION_MODE_ARG" ] && echo "Motion Mode:         $MOTION_MODE_ARG"

# ------------------------------------------------------------
# Experiment Configuration (Article 03)
# ------------------------------------------------------------
configure_experiment() {

    # Default producer parameters
    EVENT_RATE=50
    TIMESTAMP_PATTERN="realtime"
    EVENT_RATE_PATTERN="constant"
    GEOMETRY_PATTERN="random"
    MOTION_MODE="straight"
    KEYS=50

    # Burst defaults
    BURST_RATE=100
    BURST_DURATION_MS=10000
    PAUSE_MS=5000

    # Wave defaults
    WAVE_MIN=5
    WAVE_MAX=100
    WAVE_PERIOD_MS=10000

    # Skew / late defaults
    MAX_SKEW_MS=2000
    MAX_DELAY_MS=5000

    # CLI overrides
    [ -n "$TIMESTAMP_PATTERN_ARG" ] && TIMESTAMP_PATTERN="$TIMESTAMP_PATTERN_ARG"
    [ -n "$EVENT_RATE_PATTERN_ARG" ] && EVENT_RATE_PATTERN="$EVENT_RATE_PATTERN_ARG"
    [ -n "$GEOMETRY_PATTERN_ARG" ] && GEOMETRY_PATTERN="$GEOMETRY_PATTERN_ARG"
    [ -n "$MOTION_MODE_ARG" ] && MOTION_MODE="$MOTION_MODE_ARG"

    # Normalize timestamp-related params
    case "$TIMESTAMP_PATTERN" in
        skewed)
            # use MAX_SKEW_MS, ignore MAX_DELAY_MS
            ;;
        late)
            # use MAX_DELAY_MS, ignore MAX_SKEW_MS
            ;;
        *)
            # realtime: neither skew nor delay
            MAX_SKEW_MS=0
            MAX_DELAY_MS=0
            ;;
    esac
}

# ------------------------------------------------------------
# Export ENV for Producer
# ------------------------------------------------------------
export_experiment_env() {

    export EVENT_RATE
    export EVENT_RATE_PATTERN
    export TIMESTAMP_PATTERN
    export GEOMETRY_PATTERN
    export MOTION_MODE
    export KEYS

    # Burst parameters
    export BURST_RATE
    export BURST_DURATION_MS
    export PAUSE_MS

    # Wave parameters
    export WAVE_MIN
    export WAVE_MAX
    export WAVE_PERIOD_MS

    # Timestamp parameters
    export MAX_SKEW_MS
    export MAX_DELAY_MS

    echo "=== Experiment Parameters ==="
    echo "EVENT_RATE           = $EVENT_RATE"
    echo "EVENT_RATE_PATTERN   = $EVENT_RATE_PATTERN"
    echo "TIMESTAMP_PATTERN    = $TIMESTAMP_PATTERN"
    echo "GEOMETRY_PATTERN     = $GEOMETRY_PATTERN"
    echo "MOTION_MODE          = $MOTION_MODE"
    echo "KEYS                 = $KEYS"
    echo "--- Burst Mode ---"
    echo "BURST_RATE           = $BURST_RATE"
    echo "BURST_DURATION_MS    = $BURST_DURATION_MS"
    echo "PAUSE_MS             = $PAUSE_MS"
    echo "--- Wave Mode ---"
    echo "WAVE_MIN             = $WAVE_MIN"
    echo "WAVE_MAX             = $WAVE_MAX"
    echo "WAVE_PERIOD_MS       = $WAVE_PERIOD_MS"
    echo "--- Timestamp ---"
    echo "MAX_SKEW_MS          = $MAX_SKEW_MS"
    echo "MAX_DELAY_MS         = $MAX_DELAY_MS"
    echo "===================================="
}

# ------------------------------------------------------------
# Utils
# ------------------------------------------------------------
service_exists() {
    docker compose -f "$COMPOSE" config --services | grep -q "^$1$"
}

wait_for_jobmanager() {
    echo "Waiting for Flink JobManager..."

    for i in {1..20}; do
        if curl -s http://localhost:8081 >/dev/null; then
            echo "Flink JobManager ready"
            return
        fi
        sleep 3
    done

    echo "ERROR: JobManager not ready"
    exit 1
}

# ------------------------------------------------------------
# Kafka Topics
# ------------------------------------------------------------
create_kafka_topics() {
    echo "=== Creating Kafka topics ==="
    sleep 5

    docker exec kafka-1 bash -c '
        topics=("geo-events-topic:4")

        for topic in "${topics[@]}"; do
            IFS=":" read -r name partitions <<< "$topic"

            if kafka-topics.sh --bootstrap-server kafka-1:19092 --describe --topic "$name" >/dev/null 2>&1; then
                echo "Topic exists: $name"
            else
                echo "Creating topic: $name"
                kafka-topics.sh --create \
                    --topic "$name" \
                    --partitions "$partitions" \
                    --replication-factor 1 \
                    --bootstrap-server kafka-1:19092
            fi
        done
    '
}

# ------------------------------------------------------------
# Infrastructure
# ------------------------------------------------------------
start_kafka() {
    echo "=== Kafka ==="
    docker compose -f "$COMPOSE" up -d kafka-1
}

start_flink() {
    echo "=== Flink Cluster ==="
    docker compose -f "$COMPOSE" up -d jobmanager
    docker compose -f "$COMPOSE" up -d taskmanager
    wait_for_jobmanager
}

start_prometheus() {
    if service_exists prometheus; then
        echo "=== Prometheus ==="
        docker compose -f "$COMPOSE" up -d prometheus
    fi
}

start_kafdrop() {
    if service_exists kafdrop; then
        echo "=== Kafdrop ==="
        docker compose -f "$COMPOSE" up -d kafdrop
    fi
}

start_producer() {
    echo "=== Geo Producer (Spatial Methods) ==="
    docker compose -f "$COMPOSE" up -d geo_producer_spatial_methods
}

# ------------------------------------------------------------
# Start Job
# ------------------------------------------------------------
start_job() {
    echo "=== Spatial Methods Job ==="
    export_experiment_env
    docker compose -f "$COMPOSE" up -d geoflink_spatial_methods_job
}

# ------------------------------------------------------------
# MAIN
# ------------------------------------------------------------
configure_experiment

start_kafka
create_kafka_topics
start_flink
start_prometheus
start_job
start_producer

echo ""
echo "======================================"
echo "System Ready (Article 03)"
echo "======================================"
echo "Flink UI:   http://localhost:8081"
echo "Prometheus: http://localhost:9091"
echo "Kafdrop:    http://localhost:9003"

# Examples:
# ./start-system-spatial-methods.sh
# ./start-system-spatial-methods.sh skewed burst random random_walk
# ./start-system-spatial-methods.sh late wave clustered swarm
# ./start-system-spatial-methods.sh realtime constant random collision

# MOTION_MODE values: straight | random_walk | swarm | collision

