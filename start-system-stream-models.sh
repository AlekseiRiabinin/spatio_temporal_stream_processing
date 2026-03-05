#!/bin/bash
set -e

COMPOSE="docker/docker-compose.stream-models.yml"

MODEL=${1:-dataflow}
WINDOW=${2:-adaptive}
TIMESTAMP_PATTERN_ARG=${3:-}
EVENT_RATE_PATTERN_ARG=${4:-}

echo "=== Starting Stream Models System ==="
echo "Stream Model:        $MODEL"
echo "Window Strategy:     $WINDOW"
[ -n "$TIMESTAMP_PATTERN_ARG" ] && echo "Timestamp Pattern:   $TIMESTAMP_PATTERN_ARG"
[ -n "$EVENT_RATE_PATTERN_ARG" ] && echo "Rate Pattern:        $EVENT_RATE_PATTERN_ARG"

# ------------------------------------------------------------
# Canonical experiment configuration
# Core experimental knobs:
#   STREAM_MODEL
#   WINDOW_STRATEGY
#   TIMESTAMP_PATTERN
#   EVENT_RATE_PATTERN
#   EVENT_RATE
#   WINDOW_SIZE
#   WATERMARK_OUT_OF_ORDERNESS
# ------------------------------------------------------------
configure_experiment() {
    local m="$1"
    local w="$2"

    # Defaults (can be overridden per regime)
    EVENT_RATE=50
    WINDOW_SIZE=5
    COUNT_THRESHOLD=100
    PROCESSING_INTERVAL_MS=5000
    DENSITY_FACTOR=1.0
    WATERMARK_OUT_OF_ORDERNESS=0
    MICROBATCH_WM_DELAY_MS=5000

    # Default patterns (can be overridden by CLI args)
    TIMESTAMP_PATTERN="realtime"
    EVENT_RATE_PATTERN="constant"

    case "$m:$w" in
        # ----------------- DATAFLOW (event-time) -----------------
        dataflow:session)
            EVENT_RATE=5
            WINDOW_SIZE=2
            TIMESTAMP_PATTERN="skewed"      # realistic jitter
            EVENT_RATE_PATTERN="constant"
            WATERMARK_OUT_OF_ORDERNESS=1    # > max skew
            ;;
        dataflow:dynamic)
            EVENT_RATE=50
            WINDOW_SIZE=5
            TIMESTAMP_PATTERN="late"        # late events
            EVENT_RATE_PATTERN="wave"       # varying load
            WATERMARK_OUT_OF_ORDERNESS=3
            ;;
        dataflow:adaptive)
            EVENT_RATE=50
            WINDOW_SIZE=5
            TIMESTAMP_PATTERN="skewed"
            EVENT_RATE_PATTERN="wave"
            WATERMARK_OUT_OF_ORDERNESS=1
            ;;
        dataflow:multitrigger)
            EVENT_RATE=50
            WINDOW_SIZE=10
            COUNT_THRESHOLD=10
            PROCESSING_INTERVAL_MS=1000
            TIMESTAMP_PATTERN="realtime"
            EVENT_RATE_PATTERN="constant"
            WATERMARK_OUT_OF_ORDERNESS=0    # PT windows, WM irrelevant
            ;;

        # ----------------- MICROBATC H (coarse ET) ---------------
        microbatch:session)
            EVENT_RATE=20
            WINDOW_SIZE=3
            TIMESTAMP_PATTERN="realtime"
            EVENT_RATE_PATTERN="constant"
            WATERMARK_OUT_OF_ORDERNESS=0
            MICROBATCH_WM_DELAY_MS=5000
            ;;
        microbatch:dynamic)
            EVENT_RATE=50
            WINDOW_SIZE=5
            TIMESTAMP_PATTERN="realtime"
            EVENT_RATE_PATTERN="constant"
            WATERMARK_OUT_OF_ORDERNESS=0
            MICROBATCH_WM_DELAY_MS=5000
            ;;
        microbatch:adaptive)
            EVENT_RATE=50
            WINDOW_SIZE=5
            TIMESTAMP_PATTERN="realtime"
            EVENT_RATE_PATTERN="wave"
            WATERMARK_OUT_OF_ORDERNESS=0
            MICROBATCH_WM_DELAY_MS=5000
            ;;
        microbatch:multitrigger)
            EVENT_RATE=50
            WINDOW_SIZE=10
            COUNT_THRESHOLD=10
            PROCESSING_INTERVAL_MS=1000
            TIMESTAMP_PATTERN="realtime"
            EVENT_RATE_PATTERN="constant"
            WATERMARK_OUT_OF_ORDERNESS=0
            MICROBATCH_WM_DELAY_MS=5000
            ;;

        # ----------------- ACTOR (processing-time) ----------------
        actor:session)
            EVENT_RATE=20
            WINDOW_SIZE=3
            TIMESTAMP_PATTERN="realtime"
            EVENT_RATE_PATTERN="constant"
            WATERMARK_OUT_OF_ORDERNESS=0
            ;;
        actor:dynamic)
            EVENT_RATE=50
            WINDOW_SIZE=5
            TIMESTAMP_PATTERN="realtime"
            EVENT_RATE_PATTERN="wave"
            WATERMARK_OUT_OF_ORDERNESS=0
            ;;
        actor:adaptive)
            EVENT_RATE=50
            WINDOW_SIZE=5
            TIMESTAMP_PATTERN="realtime"
            EVENT_RATE_PATTERN="wave"
            WATERMARK_OUT_OF_ORDERNESS=0
            ;;
        actor:multitrigger)
            EVENT_RATE=50
            WINDOW_SIZE=10
            COUNT_THRESHOLD=10
            PROCESSING_INTERVAL_MS=1000
            TIMESTAMP_PATTERN="realtime"
            EVENT_RATE_PATTERN="constant"
            WATERMARK_OUT_OF_ORDERNESS=0
            ;;

        # ----------------- LOG (ingestion-time) -------------------
        log:session)
            EVENT_RATE=20
            WINDOW_SIZE=3
            TIMESTAMP_PATTERN="realtime"
            EVENT_RATE_PATTERN="constant"
            WATERMARK_OUT_OF_ORDERNESS=0
            ;;
        log:dynamic)
            EVENT_RATE=50
            WINDOW_SIZE=5
            TIMESTAMP_PATTERN="realtime"
            EVENT_RATE_PATTERN="constant"
            WATERMARK_OUT_OF_ORDERNESS=0
            ;;
        log:adaptive)
            EVENT_RATE=50
            WINDOW_SIZE=5
            TIMESTAMP_PATTERN="realtime"
            EVENT_RATE_PATTERN="wave"
            WATERMARK_OUT_OF_ORDERNESS=0
            ;;
        log:multitrigger)
            EVENT_RATE=50
            WINDOW_SIZE=10
            COUNT_THRESHOLD=10
            PROCESSING_INTERVAL_MS=1000
            TIMESTAMP_PATTERN="realtime"
            EVENT_RATE_PATTERN="constant"
            WATERMARK_OUT_OF_ORDERNESS=0
            ;;
        *)
            echo "Unknown combination: $m + $w"
            exit 1
            ;;
    esac

    if [ "$TIMESTAMP_PATTERN" = "skewed" ]; then
        MAX_SKEW_MS=500
    fi

    # CLI overrides for patterns (if provided)
    if [ -n "$TIMESTAMP_PATTERN_ARG" ]; then
        TIMESTAMP_PATTERN="$TIMESTAMP_PATTERN_ARG"
    fi
    if [ -n "$EVENT_RATE_PATTERN_ARG" ]; then
        EVENT_RATE_PATTERN="$EVENT_RATE_PATTERN_ARG"
    fi
}

# ------------------------------------------------------------
# Export experiment parameters
# ------------------------------------------------------------
export_experiment_env() {
    export STREAM_MODEL="$MODEL"
    export WINDOW_STRATEGY="$WINDOW"
    export EVENT_RATE="$EVENT_RATE"
    export WINDOW_SIZE="$WINDOW_SIZE"
    export COUNT_THRESHOLD="$COUNT_THRESHOLD"
    export PROCESSING_INTERVAL_MS="$PROCESSING_INTERVAL_MS"
    export DENSITY_FACTOR="$DENSITY_FACTOR"
    export WATERMARK_OUT_OF_ORDERNESS="$WATERMARK_OUT_OF_ORDERNESS"
    export WATERMARK_MAX_DELAY_MS=""          # optional override
    export MICROBATCH_WM_DELAY_MS="$MICROBATCH_WM_DELAY_MS"
    export TIMESTAMP_PATTERN="$TIMESTAMP_PATTERN"
    export EVENT_RATE_PATTERN="$EVENT_RATE_PATTERN"
    export MAX_SKEW_MS

    echo "=== Experiment Parameters ==="
    echo "STREAM_MODEL               = $STREAM_MODEL"
    echo "WINDOW_STRATEGY            = $WINDOW_STRATEGY"
    echo "TIMESTAMP_PATTERN          = $TIMESTAMP_PATTERN"
    echo "EVENT_RATE_PATTERN         = $EVENT_RATE_PATTERN"
    echo "EVENT_RATE                 = $EVENT_RATE"
    echo "WINDOW_SIZE                = $WINDOW_SIZE"
    echo "COUNT_THRESHOLD            = $COUNT_THRESHOLD"
    echo "PROCESSING_INTERVAL_MS     = $PROCESSING_INTERVAL_MS"
    echo "DENSITY_FACTOR             = $DENSITY_FACTOR"
    echo "WATERMARK_OUT_OF_ORDERNESS = $WATERMARK_OUT_OF_ORDERNESS"
    echo "MICROBATCH_WM_DELAY_MS     = $MICROBATCH_WM_DELAY_MS"
    echo "MAX_SKEW_MS                = $MAX_SKEW_MS"
    echo "===================================="
}

# ------------------------------------------------------------
# Utility: check if service exists
# ------------------------------------------------------------
service_exists() {
    docker compose -f "$COMPOSE" config --services | grep -q "^$1$"
}

# ------------------------------------------------------------
# Utility: wait for service
# ------------------------------------------------------------
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
# Kafka topics
# ------------------------------------------------------------
create_kafka_topics() {
    echo "=== Creating Kafka topics ==="
    sleep 5

    docker exec kafka-1 bash -c '
        topics=("spatial-events:4")

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
    echo "=== Geo Producer ==="
    docker compose -f "$COMPOSE" up -d geo_producer_stream_models
}

# ------------------------------------------------------------
# Start Flink Job
# ------------------------------------------------------------
start_job() {
    echo "=== Stream Model Job ==="
    export_experiment_env
    docker compose -f "$COMPOSE" up -d geoflink_stream_models_job
}

# ------------------------------------------------------------
# Canonical 16-experiment matrix
#   4 models × 4 window strategies
#   Each with a fixed canonical timestamp + rate pattern
# ------------------------------------------------------------
run_matrix() {
    MODELS=("dataflow" "microbatch" "actor" "log")
    WINDOWS=("session" "dynamic" "adaptive" "multitrigger")

    for m in "${MODELS[@]}"; do
        for w in "${WINDOWS[@]}"; do
            MODEL="$m"
            WINDOW="$w"

            echo ""
            echo "===================================="
            echo "Running canonical regime: $m + $w"
            echo "===================================="

            configure_experiment "$MODEL" "$WINDOW"
            export_experiment_env

            docker compose -f "$COMPOSE" up -d geoflink_stream_models_job

            # Let it run for some time (adjust as needed)
            sleep 20

            docker compose -f "$COMPOSE" stop geoflink_stream_models_job
        done
    done
}

# ------------------------------------------------------------
# MAIN
# ------------------------------------------------------------
if [ "$MODEL" = "matrix" ]; then
    # Matrix mode: run canonical 16 regimes
    start_kafka
    create_kafka_topics
    start_flink
    start_prometheus
    # start_kafdrop
    start_producer

    run_matrix
else
    # Single experiment mode
    configure_experiment "$MODEL" "$WINDOW"

    start_kafka
    create_kafka_topics
    start_flink
    start_prometheus
    # start_kafdrop
    start_job
    start_producer
fi

echo ""
echo "======================================"
echo "System Ready"
echo "======================================"
echo "Flink UI:   http://localhost:8081"
echo "Prometheus: http://localhost:9091"
echo "Kafdrop:    http://localhost:9003"

# Examples:
#   ./start-system-stream-models.sh dataflow session
#   ./start-system-stream-models.sh dataflow adaptive skewed wave
#   ./start-system-stream-models.sh matrix
