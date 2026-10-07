#!/usr/bin/env python3
"""Probe producer for Experiment 1 (Kafka-to-Kafka latency with transformations).

Emits raw events to the THREE input topics that the transformation pipeline
consumes:

    FIN      -> stream_fs.test.intellinx_antifraud_dbo_fin_transactions
    NOFIN    -> stream_fs.test.intellinx_antifraud_dbo_nofin_transactions
    INCOMING -> stream_fs.test.intellinx_antifraud_dbo_incoming_payments

Each event is a raw payload matching the schemas in precalculate/events.py.

Field mappings (extracted from stream_features_precalculate.py):
    FIN       key=pay_client_pin       ts=transaction_ts
    NOFIN     key=client_uid           ts=date
    INCOMING  key=recv_client_pin      ts=event_ts

Usage:
    python3 probe_producer_exp1.py [--warmup 5] [--count 100] [--sleep 0.2]
        [--fin-only | --nofin-only | --incoming-only]

By default, all three topics are targeted. Each topic gets its own
kafka-console-producer subprocess. Events are emitted round-robin across
topics so that the transformation sees interleaved activity per key.

Example:
    python3 probe_producer_exp1.py --warmup 5 --count 100 --sleep 0.2 --pause 2.0
"""

import argparse
import json
import subprocess
import sys
import time


# ---------------------------------------------------------------------------
# Topic names (kept in sync with precalculate/config.py defaults)
# ---------------------------------------------------------------------------
TOPIC_FIN = "stream_fs.test.intellinx_antifraud_dbo_fin_transactions"
TOPIC_NOFIN = "stream_fs.test.intellinx_antifraud_dbo_nofin_transactions"
TOPIC_INCOMING = "stream_fs.test.intellinx_antifraud_dbo_incoming_payments"

BOOTSTRAP = "kafka-1:19092"
KAFKA_CONTAINER = "kafka-1"
KAFKA_PRODUCER = "/opt/kafka/bin/kafka-console-producer.sh"


# ---------------------------------------------------------------------------
# Payload builders — one per event schema
# ---------------------------------------------------------------------------
def make_fin_event(run_ts: int, i: int, phase: str) -> dict:
    """Raw FIN event. Keyed by pay_client_pin, timed by transaction_ts."""
    now_ms = int(time.time() * 1000)
    return {
        "pay_client_pin": f"user-{i:03d}",
        "trans_id": f"t{run_ts}-fin-{phase}-{i}",
        "transaction_ts": now_ms,
        "amount": 100.0 + i,
        "recv_bin": "123456",
        "pay_operation_type": "C16" if i % 2 == 0 else "C21",
        "pay_account_type_mnemonic": "EG",
        "pay_phone_input_type": "MANUALLY" if i % 3 == 0 else "FROM_CONTACTS",
        "pay_add_info_list": f"fin-info-{i}",
    }


def make_nofin_event(run_ts: int, i: int, phase: str) -> dict:
    """Raw NOFIN event. Keyed by client_uid, timed by date."""
    now_ms = int(time.time() * 1000)
    return {
        "client_uid": f"user-{i:03d}",       # <-- routing key for NOFIN
        "trans_id": f"t{run_ts}-nofin-{phase}-{i}",
        "date": now_ms,                       # <-- event time field for NOFIN
        "type": "LOGIN" if i % 2 == 0 else "LOGOUT",
    }


def make_incoming_event(run_ts: int, i: int, phase: str) -> dict:
    """Raw INCOMING event. Keyed by recv_client_pin, timed by event_ts."""
    now_ms = int(time.time() * 1000)
    return {
        "recv_client_pin": f"user-{i:03d}",   # <-- routing key for INCOMING
        "trans_id": f"t{run_ts}-inc-{phase}-{i}",
        "event_ts": now_ms,                   # <-- event time field for INCOMING
        "amount": 50.0 + i,
        "oper_type_mnemonics": "C16" if i % 2 == 0 else "C21",
    }


# ---------------------------------------------------------------------------
# One console-producer subprocess per topic
# ---------------------------------------------------------------------------
class TopicProducer:
    """Manages a kafka-console-producer subprocess for one topic."""

    def __init__(self, topic: str) -> None:
        self.topic = topic
        self.proc = subprocess.Popen(
            [
                "docker", "exec", "-i", KAFKA_CONTAINER,
                KAFKA_PRODUCER,
                "--bootstrap-server", BOOTSTRAP,
                "--timeout", "0",              # flush immediately (linger.ms=0)
                "--topic", topic,
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )

    def send(self, row: dict) -> None:
        assert self.proc.stdin is not None
        line = json.dumps(row, separators=(",", ":")) + "\n"
        self.proc.stdin.write(line.encode("utf-8"))
        self.proc.stdin.flush()

    def close(self) -> None:
        if self.proc.stdin is not None:
            try:
                self.proc.stdin.close()
            except BrokenPipeError:
                pass
        try:
            self.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.proc.terminate()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--warmup", type=int, default=5,
                    help="number of warmup events per topic (discarded from stats)")
    ap.add_argument("--count", type=int, default=100,
                    help="number of measured events per topic")
    ap.add_argument("--sleep", type=float, default=0.2,
                    help="sleep between emitted events, seconds")
    ap.add_argument("--pause", type=float, default=2.0,
                    help="pause between warmup and measured phase, seconds")
    ap.add_argument("--fin-only", action="store_true",
                    help="emit only to the FIN topic")
    ap.add_argument("--nofin-only", action="store_true",
                    help="emit only to the NOFIN topic")
    ap.add_argument("--incoming-only", action="store_true",
                    help="emit only to the INCOMING topic")
    args = ap.parse_args()

    # Decide which topics to target
    only_flags = [args.fin_only, args.nofin_only, args.incoming_only]
    if any(only_flags):
        targets = []
        if args.fin_only:
            targets.append(("fin", TOPIC_FIN, make_fin_event))
        if args.nofin_only:
            targets.append(("nofin", TOPIC_NOFIN, make_nofin_event))
        if args.incoming_only:
            targets.append(("incoming", TOPIC_INCOMING, make_incoming_event))
    else:
        targets = [
            ("fin", TOPIC_FIN, make_fin_event),
            ("nofin", TOPIC_NOFIN, make_nofin_event),
            ("incoming", TOPIC_INCOMING, make_incoming_event),
        ]

    print(f"[producer] targets: {[t[0] for t in targets]}", file=sys.stderr)

    producers = {name: TopicProducer(topic) for name, topic, _ in targets}

    run_ts = int(time.time() * 1000)

    try:
        # Phase 1: warmup — one event per topic per iteration, interleaved
        for i in range(args.warmup):
            for name, _topic, builder in targets:
                producers[name].send(builder(run_ts, i, "warmup"))
            time.sleep(args.sleep)

        time.sleep(args.pause)

        # Phase 2: measured — one event per topic per iteration
        for i in range(args.count):
            for name, _topic, builder in targets:
                producers[name].send(builder(run_ts, i, "measure"))
            time.sleep(args.sleep)
    finally:
        for p in producers.values():
            p.close()


if __name__ == "__main__":
    main()


# python3 ~/Projects/spatio_temporal_stream_processing/probe_producer_exp1.py \
#   --fin-only --warmup 5 --count 100 --sleep 0.2 --pause 2.0
