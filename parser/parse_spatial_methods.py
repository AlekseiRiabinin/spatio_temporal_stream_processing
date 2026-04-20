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
    r"^(realtime|skewed|late)_(constant|wave|burst)_.+_[0-9]{8}_[0-9]{6}\.log$"
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
    r"\[COLLISION\]\s+summary\s+events=(\d+).*?collisions=(\d+).*?timeMs=([\d\.]+)"
)

PAT_CONFLICT_SUMMARY = re.compile(
    r"\[CONFLICT\]\s+summary\s+events=(\d+)\s+conflicts=(\d+)\s+timeMs=([\d\.]+)"
)

PAT_DENSITY = re.compile(
    r"\[DENSITY\]\s+global\s+count=(\d+)\s+totalAreaKm2=([\d\.]+)\s+density=([\d\.]+)"
)

PAT_TRAJ_START = re.compile(
    r"\[TRAJECTORY\]\s+start\s+objectId=(\S+)\s+events=(\d+)\s+ts=(\d+)"
)

PAT_TRAJ_UPDATE = re.compile(
    r"\[TRAJECTORY\]\s+update\s+objectId=(\S+)\s+length=(\d+)\s+timeGap=(\d+)\s+lastTs=(\d+)\s+newTs=(\d+)"
)

PAT_COLLISION_PAIR = re.compile(
    r"\[COLLISION\]\s+pair=\((\S+),(\S+)\)\s+distance=([\d\.]+).*?ttc=([\d\.]+).*?relativeSpeed=([\d\.]+)"
)

PAT_COLLISION_DETECTED = re.compile(
    r"\[COLLISION\]\s+detected\s+pair=\((\S+),(\S+)\)\s+distance=([\d\.]+).*?ttc=([\d\.]+)"
)

PAT_COLLISION_START = re.compile(
    r"\[COLLISION\]\s+action=start\s+events=(\d+)"
)

PAT_COLLISION_SIB = re.compile(
    r"\[COLLISION\]\s+action=spatialIndexBuilt\s+size=(\d+)"
)

PAT_CONFLICT_PAIR = re.compile(
    r"\[CONFLICT\]\s+(detected|skipZeroSpeed)\s+pair=\((\S+),(\S+)\).*?t=([\d\.]+).*?distance=([\d\.]+)"
)

PAT_SWARM_START = re.compile(
    r"\[SWARM\]\s+action=start\s+events=(\d+).*?eps=([\d\.]+).*?minPoints=(\d+)"
)

PAT_SWARM_SIB = re.compile(
    r"\[SWARM\]\s+action=spatialIndexBuilt\s+size=(\d+)"
)

PAT_SWARM_SEED = re.compile(
    r"\[SWARM\]\s+seed=(\S+)\s+neighbors=(\d+)"
)

PAT_SWARM_EXPAND = re.compile(
    r"\[SWARM\]\s+expand\s+current=(\S+)\s+neighbors=(\d+)"
)

PAT_PROX_PAIR = re.compile(
    r"\[PROXIMITY\]\s+pair=\((\S+),(\S+)\)\s+distance=([\d\.]+)\s+threshold=([\d\.]+)"
)

PAT_KNN = re.compile(
    r"\[KNN\]\s+action=findKNN\s+k=(\d+)\s+candidates=(\d+)\s+distanceComputations=(\d+)\s+returned=(\d+)\s+timeMs=([\d\.]+)\s+queryLat=([\d\.\-]+)\s+queryLon=([\d\.\-]+)"
)


# ---------------------------------------------------------
# HELPERS
# ---------------------------------------------------------

def parse_metadata_from_filename(fname):
    """
    Supports filenames like:
      realtime_constant_straight_20260419_133425.log
      realtime_constant_collision_20260419_140945.log
      realtime_wave_random_walk_20260419_134609.log
      realtime_burst_swarm_20260419_135812.log
      realtime_burst_swarm_clustered_20260419_135812.log
    """

    base = fname.replace(".log", "")
    parts = base.split("_")

    # Required fields
    TP = parts[0]            # realtime | skewed | late
    RP = parts[1]            # constant | wave | burst

    # Motion mode is now explicit in filename
    MM = parts[2]            # straight | collision | random_walk | swarm

    # Optional geometry field
    if parts[3].isdigit():
        GEO = "random"       # default if not provided
        date = parts[3]
        time = parts[4]
    else:
        GEO = parts[3]       # random | clustered
        date = parts[4]
        time = parts[5]

    # KEYS: 100 for swarm or collision, else 50
    if MM in ("swarm", "collision"):
        KEYS = 100
    else:
        KEYS = 50

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
                w = get_writer("density", ["count","totalAreaKm2","density"])
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

            # COLLISION PAIR
            m = PAT_COLLISION_PAIR.search(line)
            if m:
                w = get_writer("collision_pair",
                            ["objA","objB","distance","ttc","relativeSpeed"])
                w.writerow(list(m.groups()) + list(meta.values()))
                continue

            # COLLISION DETECTED (explicit)
            m = PAT_COLLISION_DETECTED.search(line)
            if m:
                w = get_writer("collision_detected",
                            ["objA","objB","distance","ttc"])
                w.writerow(list(m.groups()) + list(meta.values()))
                continue

            # COLLISION START
            m = PAT_COLLISION_START.search(line)
            if m:
                w = get_writer("collision_start", ["events"])
                w.writerow(list(m.groups()) + list(meta.values()))
                continue

            # COLLISION SPATIAL INDEX BUILT
            m = PAT_COLLISION_SIB.search(line)
            if m:
                w = get_writer("collision_spatial_index_built", ["size"])
                w.writerow(list(m.groups()) + list(meta.values()))
                continue

            # CONFLICT PAIR
            m = PAT_CONFLICT_PAIR.search(line)
            if m:
                w = get_writer("conflict_pair",
                            ["type","objA","objB","t","distance"])
                w.writerow(list(m.groups()) + list(meta.values()))
                continue

            # SWARM START
            m = PAT_SWARM_START.search(line)
            if m:
                w = get_writer("swarm_start", ["events","eps","minPoints"])
                w.writerow(list(m.groups()) + list(meta.values()))
                continue

            # SWARM SPATIAL INDEX BUILT
            m = PAT_SWARM_SIB.search(line)
            if m:
                w = get_writer("swarm_spatial_index_built", ["size"])
                w.writerow(list(m.groups()) + list(meta.values()))
                continue

            # SWARM SEED
            m = PAT_SWARM_SEED.search(line)
            if m:
                w = get_writer("swarm_seed", ["objectId","neighbors"])
                w.writerow(list(m.groups()) + list(meta.values()))
                continue

            # SWARM EXPAND
            m = PAT_SWARM_EXPAND.search(line)
            if m:
                w = get_writer("swarm_expand", ["objectId","neighbors"])
                w.writerow(list(m.groups()) + list(meta.values()))
                continue

            # PROXIMITY PAIR
            m = PAT_PROX_PAIR.search(line)
            if m:
                w = get_writer("proximity_pair",
                            ["objA","objB","distance","threshold"])
                w.writerow(list(m.groups()) + list(meta.values()))
                continue

            # KNN
            m = PAT_KNN.search(line)
            if m:
                w = get_writer("knn", [
                    "k", "candidates", "distanceComputations", "returned",
                    "timeMs", "queryLat", "queryLon"
                ])
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
