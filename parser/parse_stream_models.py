import csv
import re
from pathlib import Path

# ============================================================
# CONFIGURATION
# ============================================================

LOG_DIR = Path("../logs")

OUT_DIR = Path("../data/article_2/parsed")
META_DIR = Path("../data/article_2/metadata")

OUT_DIR.mkdir(parents=True, exist_ok=True)
META_DIR.mkdir(parents=True, exist_ok=True)

# Updated filename pattern:
# <processing>_<window>_<timestampPattern>_<eventRatePattern>_<geometryPattern>_<dateTime>.log
PAT_FILENAME = re.compile(
    r"^(actor|dataflow|microbatch|log)_"
    r"(session|dynamic|adaptive)_"
    r"(realtime|skewed|late)_"
    r"(constant|burst|wave)_"
    r"(random|clustered)_"
    r"([0-9]{8})_([0-9]{6})\.log$"
)


# ============================================================
# REGEX PATTERNS
# ============================================================

PAT_TIMESTAMP = re.compile(
    r"\[TIMESTAMP\]\s+eventTime=(\d+)\s+processingTime=(\d+)\s+lag=(-?\d+)"
)


PAT_WATERMARK = re.compile(
    r"\[WATERMARK\]\s*(.*?)(?=\n\s*\n|\n\[|\Z)",
    re.DOTALL
)


PAT_WINDOW = re.compile(
    r"\[WINDOW RESULT\]\s*(.*?)(?=\n\s*\n|\n\[|\Z)",
    re.DOTALL
)


FIELD = lambda name: re.compile(
    rf"{name}\s*=\s*(\S+)"
)


# ============================================================
# METADATA
# ============================================================

def parse_metadata_from_filename(filename):

    m = PAT_FILENAME.match(filename)
    if not m:
        raise ValueError(f"Unsupported filename: {filename}")

    (
        processing,
        window,
        timestamp,
        eventrate,
        geometry,
        date,
        time
    ) = m.groups()

    processing_map = {
        "dataflow": "continuous",
        "microbatch": "micro-batch",
        "actor": "actor-based",
        "log": "log-based"
    }

    return {
        "processing": processing_map[processing],
        "window": window,
        "timestamp": timestamp,
        "eventrate": eventrate,
        "geometry": geometry,
        "date": date,
        "time": time
    }


# ============================================================
# WRITER FACTORY
# ============================================================

def create_writer(metric, header):

    out = OUT_DIR / metric
    out.mkdir(parents=True, exist_ok=True)

    f = open(out / f"{CURRENT_FILE.stem}.csv", "w", newline="")
    writer = csv.writer(f)
    writer.writerow(header)

    return f, writer

# ============================================================
# PARSER
# ============================================================

def parse_log_file(path):

    global CURRENT_FILE
    CURRENT_FILE = path

    meta = parse_metadata_from_filename(path.name)

    timestamp_file, timestamp_writer = create_writer(
        "timestamps",
        [
            "eventTime",
            "processingTime",
            "eventLatency",
            "loggedLag",
            "processing",
            "window",
            "timestamp",
            "eventrate",
            "geometry"
        ]
    )

    watermark_file, watermark_writer = create_writer(
        "watermarks",
        [
            "watermark",
            "currentTime",
            "maxTs",
            "nextWindowEnd",
            "outOfOrderMs",
            "synthetic",
            "monotonic",
            "batchSizeMs",
            "processing",
            "window",
            "timestamp",
            "eventrate",
            "geometry"
        ]
    )

    window_file, window_writer = create_writer(
        "windows",
        [
            "key",
            "windowStart",
            "windowEnd",
            "count",
            "emitTime",
            "windowLatency",
            "processing",
            "window",
            "timestamp",
            "eventrate",
            "geometry"
        ]
    )

    text = path.read_text()

    # ========================================================
    # TIMESTAMPS
    # ========================================================

    for m in PAT_TIMESTAMP.finditer(text):

        event_time = int(m.group(1))
        processing_time = int(m.group(2))
        logged_lag = int(m.group(3))

        latency = processing_time - event_time

        timestamp_writer.writerow([
            event_time,
            processing_time,
            latency,
            logged_lag,
            meta["processing"],
            meta["window"],
            meta["timestamp"],
            meta["eventrate"],
            meta["geometry"],
            meta["date"],
            meta["time"]
        ])


    # ========================================================
    # WATERMARKS
    # ========================================================

    for block in PAT_WATERMARK.findall(text):

        def value(field):
            m = FIELD(field).search(block)
            return m.group(1) if m else ""

        watermark_writer.writerow([
            value("watermark"),
            value("currentTime"),
            value("maxTs"),
            value("nextWindowEnd"),
            value("outOfOrderMs"),
            value("synthetic"),
            value("monotonic"),
            value("batchSizeMs"),
            meta["processing"],
            meta["window"],
            meta["timestamp"],
            meta["eventrate"],
            meta["geometry"],
            meta["date"],
            meta["time"]
        ])


    # ========================================================
    # WINDOW RESULTS
    # ========================================================

    for block in PAT_WINDOW.findall(text):

        key = FIELD("key").search(block)
        ws = FIELD("windowStart").search(block)
        we = FIELD("windowEnd").search(block)
        count = FIELD("count").search(block)
        ts = FIELD("timestamp").search(block)

        if not (key and ws and we and count and ts):
            continue

        emit_time = int(ts.group(1))
        window_end = int(we.group(1))

        window_latency = emit_time - window_end

        window_writer.writerow([
            key.group(1),
            ws.group(1),
            we.group(1),
            count.group(1),
            emit_time,
            window_latency,
            meta["processing"],
            meta["window"],
            meta["timestamp"],
            meta["eventrate"],
            meta["geometry"],
            meta["date"],
            meta["time"]
        ])


    timestamp_file.close()
    watermark_file.close()
    window_file.close()

    return meta

# ============================================================
# MAIN
# ============================================================

def main():

    metadata_rows = []

    for log in sorted(LOG_DIR.glob("*.log")):

        if not PAT_FILENAME.match(log.name):
            print(f"Skipping {log.name}")
            continue

        print(f"Parsing {log.name}")

        meta = parse_log_file(log)

        metadata_rows.append({
            "file": log.name,
            **meta
        })

    meta_file = META_DIR / "experiments.csv"

    with open(meta_file, "w", newline="") as f:

        writer = csv.writer(f)

        header = [
            "file",
            "processing",
            "window",
            "timestamp",
            "eventrate",
            "geometry",
            "date",
            "time"
        ]

        writer.writerow(header)

        for row in metadata_rows:
            writer.writerow([row[h] for h in header])

    print("Finished parsing all experiments.")

if __name__ == "__main__":
    main()
