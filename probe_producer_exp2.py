#!/usr/bin/env python3
"""Probe producer for Experiment 2 (Kafka-to-Cassandra latency).

Sends N payloads to stream_fs.test.probe_serving_rows with configurable
spacing and an optional warmup phase that is tagged separately (so its
metrics can be filtered out).

Usage:
    python3 probe_producer.py [--warmup 5] [--count 100] [--sleep 0.2]

Output is line-buffered JSON — pipe it into kafka-console-producer.
"""

import argparse
import json
import sys
import time

COLUMNS = (
    'pay_client_pin', 'recv_bank_bin_set_360min', 'pay_operation_type_set_360min',
    'total_manually_payment_360minsbp', 'pay_amount_total_360minsbp',
    'recv_bank_bin_set_180min', 'pay_amount_min_180min',
    'eg_payment_count_180minsbp', 'pay_amount_total_180minsbp',
    'pay_amount_total_180minqr', 'pay_add_info_list_180minqr',
    'pay_operation_type_set_60min', 'pay_amount_total_60minsbp',
    'from_contacts_count_60minsbp', 'pay_amount_total_60minqr',
    'ev_oper_type_mnemonics_set_360minnf', 'ev_oper_type_mnemonics_set_180minnf',
    'ev_oper_type_mnemonics_set_60minnf', 'pay_amount_avg_360mininc',
    'oper_type_mnemonics_set_60mininc', 'last_trans_id', 'last_event_ts',
    'updated_at', 'events_in_window', 'events_in_window_nf',
    'events_in_window_inc', 'prev_recv_bank_bin_set_360min',
    'prev_pay_operation_type_set_360min', 'prev_total_manually_payment_360minsbp',
    'prev_pay_amount_total_360minsbp', 'prev_recv_bank_bin_set_180min',
    'prev_pay_amount_min_180min', 'prev_eg_payment_count_180minsbp',
    'prev_pay_amount_total_180minsbp', 'prev_pay_amount_total_180minqr',
    'prev_pay_add_info_list_180minqr', 'prev_pay_operation_type_set_60min',
    'prev_pay_amount_total_60minsbp', 'prev_from_contacts_count_60minsbp',
    'prev_pay_amount_total_60minqr', 'prev_last_trans_id', 'prev_events_in_window',
)


def make_row(run_ts: int, i: int, phase: str) -> dict:
    """phase is 'warmup' or 'measure' — embedded in last_trans_id."""
    row = {c: None for c in COLUMNS}
    row.update({
        "pay_client_pin": f"user-{i:03d}",
        "last_trans_id": f"t{run_ts}-{phase}-{i}",
        "last_event_ts": run_ts + i,
        "updated_at": run_ts + i,
        "events_in_window": 1,
        "events_in_window_nf": 0,
        "events_in_window_inc": 0,
    })
    return row


def emit(row: dict) -> None:
    sys.stdout.write(json.dumps(row) + "\n")
    sys.stdout.flush()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--warmup", type=int, default=5,
                    help="number of warmup messages (discarded from stats)")
    ap.add_argument("--count", type=int, default=100,
                    help="number of measured messages")
    ap.add_argument("--sleep", type=float, default=0.2,
                    help="sleep between messages, seconds")
    ap.add_argument("--pause", type=float, default=2.0,
                    help="pause between warmup and measured phase, seconds")
    args = ap.parse_args()

    run_ts = int(time.time() * 1000)

    # Phase 1: warmup — tag these so they can be excluded from stats
    for i in range(args.warmup):
        emit(make_row(run_ts, i, "warmup"))
        time.sleep(args.sleep)

    # Pause to let the probe drain the warmup batch and settle
    time.sleep(args.pause)

    # Phase 2: measured — tag these with run_ts so they're easy to filter
    for i in range(args.count):
        emit(make_row(run_ts, i, "measure"))
        time.sleep(args.sleep)


if __name__ == "__main__":
    main()



# python3 ~/Projects/spatio_temporal_stream_processing/probe_producer_exp2.py \
#   --warmup 5 --count 100 --sleep 0.2 --pause 2.0 \
#   | docker exec -i kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
#       --bootstrap-server kafka-1:19092 \
#       --topic stream_fs.test.probe_serving_rows
