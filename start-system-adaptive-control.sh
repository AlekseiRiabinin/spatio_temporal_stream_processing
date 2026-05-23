#!/bin/bash
set -e

COMPOSE="docker/docker-compose.adaptive-control.yml"

TIMESTAMP_PATTERN_ARG=${1:-}
EVENT_RATE_PATTERN_ARG=${2:-}
MOTION_MODE_ARG=${3:-}
ADAPTIVE_MODE_ARG=${4:-}

echo "=== Starting Adaptive Control System (Article 04) ==="

[ -n "$TIMESTAMP_PATTERN_ARG" ] && echo "Timestamp Pattern:   $TIMESTAMP_PATTERN_ARG"
[ -n "$EVENT_RATE_PATTERN_ARG" ] && echo "Rate Pattern:        $EVENT_RATE_PATTERN_ARG"
[ -n "$MOTION_MODE_ARG" ] && echo "Motion Mode:         $MOTION_MODE_ARG"
[ -n "$ADAPTIVE_MODE_ARG" ] && echo "Adaptive Mode:       $ADAPTIVE_MODE_ARG"

# ============================================================
# EXPERIMENT CONFIGURATION
# ============================================================
configure_experiment() {

    # --------------------------------------------------------
    # Base (from Article 3 — DO NOT CHANGE)
    # --------------------------------------------------------
    EVENT_RATE=50
    TIMESTAMP_PATTERN="realtime"
    EVENT_RATE_PATTERN="constant"
    GEOMETRY_PATTERN="random"
    MOTION_MODE="straight"
    KEYS=50

    BURST_RATE=100
    BURST_DURATION_MS=10000
    PAUSE_MS=5000

    WAVE_MIN=5
    WAVE_MAX=100
    WAVE_PERIOD_MS=10000

    MAX_SKEW_MS=2000
    MAX_DELAY_MS=5000

    # --------------------------------------------------------
    # Article 4 ADDITIONS (Adaptive Control Layer)
    # --------------------------------------------------------

    # Windowing control
    WINDOW_STRATEGY="fixed"
    WINDOW_SIZE_MS=5000

    # Watermark control
    WATERMARK_STRATEGY="fixed"
    WATERMARK_DELAY_MS=3000

    # ML inference mode (core novelty of Article 4)
    ML_INFERENCE="disabled"   # disabled | cpu | gpu
    MODEL_UPDATE_INTERVAL=5000

    # Feedback loop frequency
    ADAPTATION_INTERVAL_MS=2000

    # --------------------------------------------------------
    # CLI overrides
    # --------------------------------------------------------
    [ -n "$TIMESTAMP_PATTERN_ARG" ] && TIMESTAMP_PATTERN="$TIMESTAMP_PATTERN_ARG"
    [ -n "$EVENT_RATE_PATTERN_ARG" ] && EVENT_RATE_PATTERN="$EVENT_RATE_PATTERN_ARG"
    [ -n "$MOTION_MODE_ARG" ] && MOTION_MODE="$MOTION_MODE_ARG"
    [ -n "$ADAPTIVE_MODE_ARG" ] && ML_INFERENCE="$ADAPTIVE_MODE_ARG"

    # --------------------------------------------------------
    # Same experiment matrix logic as Article 3
    # --------------------------------------------------------
    case "$MOTION_MODE" in
        straight)
            GEOMETRY_PATTERN="random"
            KEYS=50
            ;;
        random_walk)
            GEOMETRY_PATTERN="random"
            KEYS=50
            ;;
        swarm)
            GEOMETRY_PATTERN="clustered"
            KEYS=100
            ;;
        collision)
            GEOMETRY_PATTERN="clustered"
            KEYS=100
            ;;
        *)
            echo "Unknown MOTION_MODE '$MOTION_MODE', using fallback"
            GEOMETRY_PATTERN="random"
            KEYS=50
            MOTION_MODE="straight"
            ;;
    esac

    # Normalize timestamp distortion logic
    case "$TIMESTAMP_PATTERN" in
        skewed)
            ;;
        late)
            ;;
        *)
            MAX_SKEW_MS=0
            MAX_DELAY_MS=0
            ;;
    esac
}

# ============================================================
# EXPORT ENVIRONMENT
# ============================================================
export_experiment_env() {

    # Base stream config
    export EVENT_RATE EVENT_RATE_PATTERN TIMESTAMP_PATTERN
    export GEOMETRY_PATTERN MOTION_MODE KEYS

    export BURST_RATE BURST_DURATION_MS PAUSE_MS
    export WAVE_MIN WAVE_MAX WAVE_PERIOD_MS

    export MAX_SKEW_MS MAX_DELAY_MS

    # Adaptive control (Article 4)
    export WINDOW_STRATEGY WINDOW_SIZE_MS
    export WATERMARK_STRATEGY WATERMARK_DELAY_MS
    export ML_INFERENCE MODEL_UPDATE_INTERVAL
    export ADAPTATION_INTERVAL_MS

    echo ""
    echo "===================================="
    echo "ADAPTIVE CONTROL CONFIGURATION"
    echo "===================================="
    echo "WINDOW_STRATEGY        = $WINDOW_STRATEGY"
    echo "WINDOW_SIZE_MS         = $WINDOW_SIZE_MS"
    echo "WATERMARK_STRATEGY     = $WATERMARK_STRATEGY"
    echo "WATERMARK_DELAY_MS     = $WATERMARK_DELAY_MS"
    echo "ML_INFERENCE           = $ML_INFERENCE"
    echo "MODEL_UPDATE_INTERVAL  = $MODEL_UPDATE_INTERVAL"
    echo "ADAPTATION_INTERVAL_MS = $ADAPTATION_INTERVAL_MS"
    echo "===================================="
}

# ============================================================
# INFRASTRUCTURE HELPERS
# ============================================================
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

# ============================================================
# KAFKA TOPIC (REUSED FROM ARTICLE 3)
# ============================================================
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

# ============================================================
# START SYSTEM COMPONENTS
# ============================================================
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

start_producer() {
    echo "=== Geo Producer (Adaptive Control System) ==="
    docker compose -f "$COMPOSE" up -d geo_producer_adaptive_control
}

start_job() {
    echo "=== Flink Adaptive Control Job ==="
    export_experiment_env
    docker compose -f "$COMPOSE" up -d flink_adaptive_control_job
}

# ============================================================
# PIPELINE EXECUTION
# ============================================================
configure_experiment
start_kafka
create_kafka_topics
start_flink
start_job
start_producer

echo ""
echo "======================================"
echo "ADAPTIVE CONTROL SYSTEM READY"
echo "======================================"
echo "Flink UI: http://localhost:8081"
