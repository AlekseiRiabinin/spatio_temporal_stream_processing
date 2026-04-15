import re
import csv
from pathlib import Path


# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------
LOG_DIR = Path("../logs")
OUT_DIR = Path("../data/article_3/parsed")
META_DIR = Path("../data/article_3/metadata")

OUT_DIR.mkdir(parents=True, exist_ok=True)
META_DIR.mkdir(parents=True, exist_ok=True)

PAT_EXPERIMENT_FILE = re.compile(
    r"^(realtime|skewed|late)_(constant|constant1|wave|burst)_[0-9]{8}_[0-9]{6}\.log$"
)

# ---------------------------------------------------------
# REGEX PATTERNS FOR EACH METRIC
# ---------------------------------------------------------

PAT_TIMESTAMP = re.compile(
    r"\[TIMESTAMP\]\s+eventTime=(\d+)\s+processingTime=(\d+)\s+lag=(\d+)\s+objectId=(\S+)"
)

PAT_WINDOW = re.compile(
    r"\[WINDOW RESULT\]\s+windowStart=(\d+)\s+windowEnd=(\d+)\s+count=(\d+)\s+processingTime=(\d+)"
)

PAT_SPATIAL_INSERT = re.compile(
    r"\[SPATIAL INDEX\]\s+action=insert\s+cell=\((\d+),(\d+)\)\s+size=(\d+)\s+objectId=(\S+)\s+lat=([\d\.\-]+)\s+lon=([\d\.\-]+)"
)

PAT_SPATIAL_QUERY = re.compile(
    r"\[SPATIAL INDEX\]\s+action=queryRadius\s+center=\((\d+),(\d+)\)\s+radius=([\d\.]+)\s+candidates=(\d+)\s+returned=(\d+)\s+timeMs=([\d\.]+)"
)

PAT_PROX_SUMMARY = re.compile(
    r"\[PROXIMITY\]\s+action=summary\s+events=(\d+)\s+neighbors=(\d+)\s+distanceComputations=(\d+)\s+interactionsRaw=(\d+)\s+interactionsFinal=(\d+)\s+timeMs=([\d\.]+)"
)

PAT_COLLISION_SUMMARY = re.compile(
    r"\[COLLISION\]\s+summary\s+events=(\d+)\s+collisions=(\d+)\s+timeMs=([\d\.]+)"
)

PAT_CONFLICT_SUMMARY = re.compile(
    r"\[CONFLICT\]\s+summary\s+events=(\d+)\s+conflicts=(\d+)\s+timeMs=([\d\.]+)"
)

PAT_DENSITY = re.compile(
    r"\[DENSITY\]\s+global\s+count=(\d+)\s+totalArea=([\d\.]+)\s+density=([\d\.]+)"
)

PAT_TRAJ_START = re.compile(
    r"\[TRAJECTORY\]\s+start\s+objectId=(\S+)\s+events=(\d+)\s+ts=(\d+)"
)

PAT_TRAJ_UPDATE = re.compile(
    r"\[TRAJECTORY\]\s+update\s+objectId=(\S+)\s+length=(\d+)\s+timeGap=(\d+)\s+lastTs=(\d+)\s+newTs=(\d+)"
)

# ---------------------------------------------------------
# HELPERS
# ---------------------------------------------------------

def parse_metadata_from_filename(fname):
    """
    Example filename:
    realtime_burst_20260415_225424.log
    realtime_constant1_20260415_230617.log  (constant1 = collision)
    """
    base = fname.replace(".log", "")
    parts = base.split("_")

    TP = parts[0]            # realtime | skewed | late
    RP = parts[1]            # constant | wave | burst | constant1
    date = parts[2]
    time = parts[3]

    # Motion mode encoded in filename:
    # constant  -> straight
    # constant1 -> collision
    # wave      -> random_walk
    # burst     -> swarm (clustered in 3,7,11)
    if RP == "constant":
        MM = "straight"
    elif RP == "constant1":
        MM = "collision"
    elif RP == "wave":
        MM = "random_walk"
    elif RP == "burst":
        MM = "swarm"
    else:
        MM = "unknown"

    # Geometry: clustered only in burst scenarios
    GEO = "clustered" if RP == "burst" else "random"

    # KEYS: 100 only in burst scenarios
    KEYS = 100 if RP == "burst" else 50

    return {
        "timestamp_pattern": TP,
        "rate_pattern": RP,
        "geometry": GEO,
        "motion_mode": MM,
        "keys": KEYS,
        "date": date,
        "time": time
    }

# ---------------------------------------------------------
# MAIN PARSER
# ---------------------------------------------------------

def parse_log_file(path):
    meta = parse_metadata_from_filename(path.name)

    # Prepare output writers
    writers = {}

    def get_writer(metric_name, header):
        out_path = OUT_DIR / metric_name / f"{path.stem}.csv"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        if metric_name not in writers:
            f = open(out_path, "w", newline="")
            w = csv.writer(f)
            w.writerow(header + list(meta.keys()))
            writers[metric_name] = (f, w)
        return writers[metric_name][1]

    with open(path, "r") as f:
        for line in f:
            line = line.strip()

            # TIMESTAMP
            m = PAT_TIMESTAMP.search(line)
            if m:
                w = get_writer("timestamp", ["eventTime","processingTime","lag","objectId"])
                w.writerow(list(m.groups()) + list(meta.values()))
                continue

            # WINDOW RESULT
            m = PAT_WINDOW.search(line)
            if m:
                w = get_writer("window", ["windowStart","windowEnd","count","processingTime"])
                w.writerow(list(m.groups()) + list(meta.values()))
                continue

            # SPATIAL INDEX INSERT
            m = PAT_SPATIAL_INSERT.search(line)
            if m:
                w = get_writer("spatial_insert", ["cell_x","cell_y","size","objectId","lat","lon"])
                w.writerow(list(m.groups()) + list(meta.values()))
                continue

            # SPATIAL INDEX QUERY
            m = PAT_SPATIAL_QUERY.search(line)
            if m:
                w = get_writer("spatial_query", ["center_x","center_y","radius","candidates","returned","timeMs"])
                w.writerow(list(m.groups()) + list(meta.values()))
                continue

            # PROXIMITY SUMMARY
            m = PAT_PROX_SUMMARY.search(line)
            if m:
                w = get_writer("proximity", ["events","neighbors","distanceComputations","interactionsRaw","interactionsFinal","timeMs"])
                w.writerow(list(m.groups()) + list(meta.values()))
                continue

            # COLLISION SUMMARY
            m = PAT_COLLISION_SUMMARY.search(line)
            if m:
                w = get_writer("collision", ["events","collisions","timeMs"])
                w.writerow(list(m.groups()) + list(meta.values()))
                continue

            # CONFLICT SUMMARY
            m = PAT_CONFLICT_SUMMARY.search(line)
            if m:
                w = get_writer("conflict", ["events","conflicts","timeMs"])
                w.writerow(list(m.groups()) + list(meta.values()))
                continue

            # DENSITY
            m = PAT_DENSITY.search(line)
            if m:
                w = get_writer("density", ["count","totalArea","density"])
                w.writerow(list(m.groups()) + list(meta.values()))
                continue

            # TRAJECTORY START
            m = PAT_TRAJ_START.search(line)
            if m:
                w = get_writer("trajectory_start", ["objectId","events","ts"])
                w.writerow(list(m.groups()) + list(meta.values()))
                continue

            # TRAJECTORY UPDATE
            m = PAT_TRAJ_UPDATE.search(line)
            if m:
                w = get_writer("trajectory_update", ["objectId","length","timeGap","lastTs","newTs"])
                w.writerow(list(m.groups()) + list(meta.values()))
                continue

    # Close all writers
    for f, w in writers.values():
        f.close()

    return meta


def main():
    META_FILE = META_DIR / "experiments.csv"
    meta_rows = []

    for log_file in LOG_DIR.glob("*.log"):
        # Only parse valid Article-03 experiment logs
        if not PAT_EXPERIMENT_FILE.match(log_file.name):
            print(f"Skipping unrelated log: {log_file.name}")
            continue

        print(f"Parsing {log_file.name} ...")
        meta = parse_log_file(log_file)
        meta_rows.append({"file": log_file.name, **meta})

    # Write metadata table
    with open(META_FILE, "w", newline="") as f:
        w = csv.writer(f)
        header = ["file","timestamp_pattern","rate_pattern","geometry","motion_mode","keys","date","time"]
        w.writerow(header)
        for row in meta_rows:
            w.writerow([row[h] for h in header])

    print("Parsing complete.")


if __name__ == "__main__":
    main()
