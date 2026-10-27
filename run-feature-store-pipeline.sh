#!/usr/bin/env bash
# Submit the transform and writer Flink jobs that make up the feature-store pipeline.
#
# Assumes:
#   - ./start-system-fs-stream.sh has already brought up kafka, cassandra,
#     flink-jobmanager-fs and flink-taskmanager-fs.
#   - ./run-feature-store-data-generator.sh is NOT yet running (or has not
#     produced anything), so the writer's "latest" offset lands on an empty hop
#     topic and the transform's "latest" offset lands on an empty input topic.
#
# Submit order (downstream first):
#   writer_job  (reads  stream_fs.test.probe_serving_rows → writes Cassandra)
#   transform_job (reads stream_fs.test.intellinx_antifraud_dbo
#                  writes stream_fs.test.probe_serving_rows)

set -euo pipefail

COMPOSE_FILE="docker/docker-compose.fs-stream.yml"
FLINK_REST_HOST="${FLINK_REST_HOST:-localhost}"
FLINK_REST_PORT="${FLINK_REST_PORT:-8082}"
FLINK_UI="http://${FLINK_REST_HOST}:${FLINK_REST_PORT}"
WAIT_JOBS_TIMEOUT=120

INPUT_TOPIC="stream_fs.test.intellinx_antifraud_dbo"
HOP_TOPIC="stream_fs.test.probe_serving_rows"
TRANSFORM_GROUP="pyflink-kafka-probe-single"
WRITER_GROUP="pyflink-cassandra-probe-single"
CASSANDRA_TABLE="test.af_dbo_v1_7"

# env.execute() names in transform_job.py and writer_job.py — update if renamed.
TRANSFORM_JOB_NAME="transform-job"
WRITER_JOB_NAME="cassandra-latency-probe"

log()  { printf '\033[1;34m[pipeline]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[pipeline]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[pipeline]\033[0m %s\n' "$*" >&2; exit 1; }

trap 'rc=$?; printf "[pipeline] EXIT rc=%d at line %d: %s\n" "$rc" "$LINENO" "$BASH_COMMAND" >&2' ERR

# ---------------------------------------------------------------------------
# 0. Preflight
# ---------------------------------------------------------------------------
log "=== Submitting Feature Store pipeline (transform_job + writer_job) ==="

[[ -f "$COMPOSE_FILE" ]] || die "Compose file not found: $COMPOSE_FILE"

if ! curl -sf "${FLINK_UI}/overview" >/dev/null; then
    die "Flink REST API is not reachable at ${FLINK_UI}. Run ./start-system-fs-stream.sh first."
fi

# /jobs/overview is the endpoint that carries 'name' and 'state'. /jobs only
# carries 'id' and 'status' (and not even 'name') in Flink 1.14–1.16.
RUNNING_NAMES=$(curl -sf "${FLINK_UI}/jobs/overview" \
    | python3 -c '
import json, sys
for j in json.load(sys.stdin).get("jobs", []):
    if j.get("state") == "RUNNING":
        print(j["name"])
' || true)

for conflicting in "$TRANSFORM_JOB_NAME" "$WRITER_JOB_NAME"; do
    if grep -qx "$conflicting" <<<"$RUNNING_NAMES"; then
        warn "A job named '$conflicting' is already RUNNING."
        warn "Cancel it in the Flink UI ($FLINK_UI) before re-submitting,"
        warn "or rename your env.execute() if you intend to run both."
        die  "Refusing to start a duplicate consumer."
    fi
done

# ---------------------------------------------------------------------------
# 1. Confirm the topics exist with the right timestamp policy
# ---------------------------------------------------------------------------
log "Checking Kafka topics..."

for topic in "$INPUT_TOPIC" "$HOP_TOPIC"; do
    if ! docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
            --bootstrap-server kafka-1:19092 --describe --topic "$topic" >/dev/null 2>&1; then
        die "Kafka topic '$topic' does not exist. Run ./start-system-fs-stream.sh (topics-init block)."
    fi
done

# `grep` returns 1 when no match; under `set -o pipefail` that would kill this
# script. For CreateTime topics there is no dynamic override, so no match is
# the normal case. `|| true` converts it to empty output, defaulted below.
for topic in "$INPUT_TOPIC" "$HOP_TOPIC"; do
    ts_type=$(
        docker exec kafka-1 /opt/kafka/bin/kafka-configs.sh \
            --bootstrap-server kafka-1:19092 --describe --entity-type topics --entity-name "$topic" 2>/dev/null \
        | grep -oE 'message\.timestamp\.type=[A-Za-z]+' \
        | head -1 \
        | cut -d= -f2 \
        || true
    )
    actual="${ts_type:-CreateTime}"
    if [[ "$actual" != "CreateTime" ]]; then
        die "Topic '$topic' has message.timestamp.type=$actual, expected CreateTime."
    fi
done
log "Topics OK: $INPUT_TOPIC, $HOP_TOPIC (both CreateTime)."

# ---------------------------------------------------------------------------
# 2. Start writer_job FIRST (downstream).
# ---------------------------------------------------------------------------
log "Starting feature_store_writer_job (downstream)..."
docker compose -f "$COMPOSE_FILE" up -d --force-recreate feature_store_writer_job

# ---------------------------------------------------------------------------
# 3. Start transform_job SECOND (upstream).
# ---------------------------------------------------------------------------
log "Starting feature_store_transform_job (upstream)..."
docker compose -f "$COMPOSE_FILE" up -d --force-recreate feature_store_transform_job

# ---------------------------------------------------------------------------
# 4. Wait for both jobs to be RUNNING.
# ---------------------------------------------------------------------------
log "Waiting for both jobs to reach RUNNING (timeout ${WAIT_JOBS_TIMEOUT}s)..."

deadline=$(( SECONDS + WAIT_JOBS_TIMEOUT ))
both_running=false
last_states=""
while (( SECONDS < deadline )); do
    last_states=$(curl -sf "${FLINK_UI}/jobs/overview" \
        | python3 -c '
import json, sys
for j in json.load(sys.stdin).get("jobs", []):
    name = j.get("name")
    state = j.get("state")
    if name and state:
        print(name + "\t" + state)
' || true)

    if grep -qxF "${TRANSFORM_JOB_NAME}"$'\t'"RUNNING" <<<"$last_states" \
       && grep -qxF "${WRITER_JOB_NAME}"$'\t'"RUNNING" <<<"$last_states"; then
        both_running=true
        log "Both jobs are RUNNING."
        break
    fi

    sleep 2
done

if [[ "$both_running" != "true" ]]; then
    warn "Current job states:"
    printf '%s\n' "$last_states" | sed 's/^/    /' >&2
    die "Timed out waiting for both jobs to reach RUNNING."
fi

# ---------------------------------------------------------------------------
# 5. Verify both consumer groups are caught up (lag = 0).
# ---------------------------------------------------------------------------
log "Checking consumer group lag..."

check_lag_zero() {
    local group="$1"
    local output
    output=$(docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
        --bootstrap-server kafka-1:19092 --describe --group "$group" 2>&1 || true)

    # Flink creates the group only after committing an offset, which only
    # happens after it has consumed at least one message. On empty topics,
    # "group does not exist" is the expected (and correct) state.
    if grep -q "Consumer group .* does not exist" <<<"$output"; then
        log "Group '$group': not yet created (empty topic — expected before first message)."
        return 0
    fi

    if [[ -z "$output" ]]; then
        warn "Consumer group '$group' returned no output."
        return 0
    fi

    local nonzero
    nonzero=$(awk 'NR>1 && $6 != "0" && $6 != "-" {print}' <<<"$output")
    if [[ -n "$nonzero" ]]; then
        warn "Group '$group' has non-zero lag:"
        printf '%s\n' "$nonzero" | sed 's/^/    /' >&2
        return 1
    fi
    log "Group '$group': lag = 0."
    return 0
}

check_lag_zero "$TRANSFORM_GROUP" || warn "transform_job has backlog — is something already producing?"
check_lag_zero "$WRITER_GROUP"    || warn "writer_job has backlog — check the hop topic."

# ---------------------------------------------------------------------------
# 6. Summary
# ---------------------------------------------------------------------------
cat <<EOF

[pipeline] Pipeline is up and listening.

  Flink UI:        ${FLINK_UI}
  Input topic:     ${INPUT_TOPIC}    (write side: feature_store_data_generator)
  Hop topic:       ${HOP_TOPIC}      (transform_job -> writer_job)
  Cassandra:       ${CASSANDRA_TABLE}  (write side: writer_job)

Next step:

  ./run-feature-store-data-generator.sh

Then watch the chain:

  docker compose -f ${COMPOSE_FILE} logs -f feature_store_transform_job
  docker compose -f ${COMPOSE_FILE} logs -f feature_store_writer_job

EOF
