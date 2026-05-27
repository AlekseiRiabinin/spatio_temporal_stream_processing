#!/bin/bash
set -e

COMPOSE="docker/docker-compose.adaptive-control.yml"

TIMESTAMP_PATTERN_ARG=${1:-}
EVENT_RATE_PATTERN_ARG=${2:-}
MOTION_MODE_ARG=${3:-}
ADAPTIVE_MODE_ARG=${4:-}

echo "=== Starting Adaptive Control System (Article 04) ==="

[ -n "$TIMESTAMP_PATTERN_ARG" ] && echo "Experiment Profile:   $TIMESTAMP_PATTERN_ARG"
[ -n "$EVENT_RATE_PATTERN_ARG" ] && echo "Rate Pattern:         $EVENT_RATE_PATTERN_ARG"
[ -n "$MOTION_MODE_ARG" ] && echo "Motion Mode:          $MOTION_MODE_ARG"
[ -n "$ADAPTIVE_MODE_ARG" ] && echo "Adaptive Mode:        $ADAPTIVE_MODE_ARG"

# ============================================================
# EXPERIMENT CONFIGURATION
# ============================================================
configure_experiment() {

    # ========================================================
    # Base Stream Configuration
    # ========================================================
    EVENT_RATE=50
    EVENT_RATE_PATTERN="constant"

    TIMESTAMP_PATTERN="realtime"   # experiment profile key

    GEOMETRY_PATTERN="random"
    MOTION_MODE="straight"

    KEYS=50

    # ========================================================
    # Burst / Wave Rate Control
    # ========================================================
    BURST_RATE=100
    BURST_DURATION_MS=10000
    PAUSE_MS=5000

    WAVE_MIN=5
    WAVE_MAX=100
    WAVE_PERIOD_MS=10000

    # ========================================================
    # Base Distortion Parameters (used by ENABLE_* layers)
    # ========================================================
    MAX_SKEW_MS=2000
    MAX_DELAY_MS=5000

    DISORDER_PROBABILITY=0.20
    MAX_DISORDER_MS=3000

    # ========================================================
    # Distortion Layers (activated by experiment profile)
    # ========================================================
    ENABLE_TIMESTAMP_SKEW=false
    TIMESTAMP_SKEW_MODE="bidirectional"
    SKEW_PROBABILITY=0.20

    ENABLE_DISORDER=false
    ENABLE_JITTER=false
    MAX_JITTER_MS=500

    ENABLE_BURST_DELAY=false
    BURST_DELAY_PROBABILITY=0.05
    BURST_DELAY_MS=10000

    ENABLE_GPS_DROPOUT=false
    GPS_DROPOUT_PROBABILITY=0.00

    # ========================================================
    # Adaptive Layer
    # ========================================================
    WINDOW_STRATEGY="fixed"
    WINDOW_SIZE_MS=5000

    WATERMARK_STRATEGY="fixed"
    WATERMARK_DELAY_MS=3000

    ML_INFERENCE="disabled"
    MODEL_UPDATE_INTERVAL=5000

    ADAPTATION_INTERVAL_MS=2000

    # ========================================================
    # CLI Overrides
    # ========================================================
    [ -n "$TIMESTAMP_PATTERN_ARG" ] && TIMESTAMP_PATTERN="$TIMESTAMP_PATTERN_ARG"
    [ -n "$EVENT_RATE_PATTERN_ARG" ] && EVENT_RATE_PATTERN="$EVENT_RATE_PATTERN_ARG"
    [ -n "$MOTION_MODE_ARG" ] && MOTION_MODE="$MOTION_MODE_ARG"
    [ -n "$ADAPTIVE_MODE_ARG" ] && ML_INFERENCE="$ADAPTIVE_MODE_ARG"

    # ========================================================
    # MOTION MODE CONFIGURATION
    # ========================================================
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

        corridor)
            GEOMETRY_PATTERN="corridor"
            KEYS=80
            ;;

        *)
            echo "Unknown MOTION_MODE '$MOTION_MODE', using fallback"
            GEOMETRY_PATTERN="random"
            KEYS=50
            MOTION_MODE="straight"
            ;;
    esac

    # ============================================================
    # EXPERIMENT PRESET EXPANSION
    # ============================================================
    # NOTE:
    # TIMESTAMP_PATTERN is now an experiment profile key.
    # It DOES NOT define timestamp semantics anymore.
    # It ONLY activates distortion configurations.

    case "$TIMESTAMP_PATTERN" in

        realtime)
            # clean baseline system (no distortions)
            ENABLE_TIMESTAMP_SKEW=false
            ENABLE_DISORDER=false
            ENABLE_JITTER=false
            ENABLE_BURST_DELAY=false
            ENABLE_GPS_DROPOUT=false
            ;;

        skewed)
            ENABLE_TIMESTAMP_SKEW=true
            ENABLE_DISORDER=false
            ENABLE_JITTER=false
            ENABLE_BURST_DELAY=false
            ENABLE_GPS_DROPOUT=false
            ;;

        late)
            ENABLE_TIMESTAMP_SKEW=false
            ENABLE_DISORDER=true
            ENABLE_BURST_DELAY=true
            ENABLE_JITTER=false
            ENABLE_GPS_DROPOUT=false
            ;;

        out_of_order|out-of-order)
            ENABLE_TIMESTAMP_SKEW=false
            ENABLE_DISORDER=true
            ENABLE_JITTER=true
            ENABLE_BURST_DELAY=false
            ENABLE_GPS_DROPOUT=false
            ;;

        mixed)
            ENABLE_TIMESTAMP_SKEW=true
            ENABLE_DISORDER=true
            ENABLE_JITTER=true
            ENABLE_BURST_DELAY=true
            ENABLE_GPS_DROPOUT=true
            GPS_DROPOUT_PROBABILITY=0.02
            ;;

        *)
            echo "Unknown EXPERIMENT PROFILE '$TIMESTAMP_PATTERN', using realtime"
            TIMESTAMP_PATTERN="realtime"

            ENABLE_TIMESTAMP_SKEW=false
            ENABLE_DISORDER=false
            ENABLE_JITTER=false
            ENABLE_BURST_DELAY=false
            ENABLE_GPS_DROPOUT=false
            ;;
    esac
}

# ============================================================
# EXPORT ENVIRONMENT
# ============================================================
export_experiment_env() {

    export EVENT_RATE
    export EVENT_RATE_PATTERN

    export TIMESTAMP_PATTERN

    export GEOMETRY_PATTERN
    export MOTION_MODE
    export KEYS

    export BURST_RATE
    export BURST_DURATION_MS
    export PAUSE_MS

    export WAVE_MIN
    export WAVE_MAX
    export WAVE_PERIOD_MS

    export MAX_SKEW_MS
    export MAX_DELAY_MS

    export DISORDER_PROBABILITY
    export MAX_DISORDER_MS

    export ENABLE_TIMESTAMP_SKEW
    export TIMESTAMP_SKEW_MODE
    export SKEW_PROBABILITY

    export ENABLE_DISORDER
    export ENABLE_JITTER
    export MAX_JITTER_MS

    export ENABLE_BURST_DELAY
    export BURST_DELAY_PROBABILITY
    export BURST_DELAY_MS

    export ENABLE_GPS_DROPOUT
    export GPS_DROPOUT_PROBABILITY

    export WINDOW_STRATEGY
    export WINDOW_SIZE_MS

    export WATERMARK_STRATEGY
    export WATERMARK_DELAY_MS

    export ML_INFERENCE
    export MODEL_UPDATE_INTERVAL

    export ADAPTATION_INTERVAL_MS

    echo ""
    echo "===================================="
    echo "EXPERIMENT CONFIGURATION"
    echo "===================================="

    echo "--- Stream ---"
    echo "EVENT_RATE_PATTERN      = $EVENT_RATE_PATTERN"
    echo "EXPERIMENT_PROFILE      = $TIMESTAMP_PATTERN"
    echo "GEOMETRY_PATTERN        = $GEOMETRY_PATTERN"
    echo "MOTION_MODE             = $MOTION_MODE"
    echo "KEYS                    = $KEYS"

    echo ""
    echo "--- Distortion Layers ---"
    echo "ENABLE_TIMESTAMP_SKEW   = $ENABLE_TIMESTAMP_SKEW"
    echo "ENABLE_DISORDER         = $ENABLE_DISORDER"
    echo "ENABLE_JITTER           = $ENABLE_JITTER"
    echo "ENABLE_BURST_DELAY      = $ENABLE_BURST_DELAY"
    echo "ENABLE_GPS_DROPOUT      = $ENABLE_GPS_DROPOUT"

    echo ""
    echo "--- Adaptive Layer ---"
    echo "WINDOW_STRATEGY         = $WINDOW_STRATEGY"
    echo "WINDOW_SIZE_MS          = $WINDOW_SIZE_MS"
    echo "WATERMARK_STRATEGY      = $WATERMARK_STRATEGY"
    echo "WATERMARK_DELAY_MS      = $WATERMARK_DELAY_MS"
    echo "ML_INFERENCE            = $ML_INFERENCE"
    echo "ADAPTATION_INTERVAL_MS  = $ADAPTATION_INTERVAL_MS"

    echo "===================================="
}


# ============================================================
# INFRASTRUCTURE HELPERS
# ============================================================
service_exists() {
    docker compose -f "$COMPOSE" config --services | grep -q "^$1$"
}

# ============================================================
# WAIT FOR FLINK
# ============================================================
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
# KAFKA TOPICS
# ============================================================
create_kafka_topics() {
    echo "=== Creating Kafka topics ==="
    sleep 5
    docker exec kafka-1 bash -c '
        topics=("spatial-events:4")
        for topic in "${topics[@]}"; do
            IFS=":" read -r name partitions <<< "$topic"

            if /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --describe --topic "$name" >/dev/null 2>&1; then
                echo "Topic exists: $name"
            else
                echo "Creating topic: $name"
                /opt/kafka/bin/kafka-topics.sh --create \
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

echo ""
echo "Examples:"
echo "./start-system-adaptive-control.sh"
echo "./start-system-adaptive-control.sh skewed swarm"
echo "./start-system-adaptive-control.sh late collision"
echo "./start-system-adaptive-control.sh mixed corridor cpu"
echo "./start-system-adaptive-control.sh out_of_order random_walk"

echo ""
echo "Motion modes:"
echo "  straight | random_walk | swarm | collision | corridor"

echo ""
echo "Experiment profiles (TIMESTAMP_PATTERN):"
echo "  realtime | skewed | late | out_of_order | mixed"

echo ""
echo "Note: These are EXPERIMENT PROFILES, not timestamp semantics."
