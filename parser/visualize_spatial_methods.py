#!/usr/bin/env python3
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pathlib import Path

from style import apply_style


BASE = Path("../data/article_3/parsed")
OUT = Path("../data/article_3/figures")
OUT.mkdir(exist_ok=True)

apply_style()

# ---------------------------------------------------------
# Helper: load all CSVs for a metric
# ---------------------------------------------------------
def load_metric(metric):
    folder = BASE / metric
    files = list(folder.glob("*.csv"))
    if not files:
        print(f"No files for metric: {metric}")
        return pd.DataFrame()
    df = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
    return df

# ---------------------------------------------------------
# 7.2.1 TIMESTAMP LAG
# ---------------------------------------------------------
def plot_timestamp_lag():
    df = load_metric("timestamp")
    if df.empty:
        return

    df["lag"] = df["lag"].astype(float)

    plt.figure()
    sns.boxplot(data=df, x="timestamp_pattern", y="lag")
    plt.title("Timestamp Lag Distribution by Timestamp Pattern")
    plt.xlabel("Timestamp Pattern")
    plt.ylabel("Lag (ms)")

    out = OUT / "timestamp"
    out.mkdir(exist_ok=True)
    plt.savefig(out / "timestamp_lag_boxplot.png")
    plt.close()

# ---------------------------------------------------------
# 7.2.2 WINDOW LATENCY
# ---------------------------------------------------------
def plot_window_latency():
    df = load_metric("window")
    if df.empty:
        return

    df["latency"] = df["processingTime"] - df["windowEnd"]

    plt.figure()
    sns.histplot(df["latency"], bins=50, kde=True)
    plt.title("Window Emission Latency Distribution")
    plt.xlabel("Latency (ms)")
    plt.ylabel("Count")

    out = OUT / "window"
    out.mkdir(exist_ok=True)
    plt.savefig(out / "window_latency_hist.png")
    plt.close()

# ---------------------------------------------------------
# 7.3.1 PROXIMITY
# ---------------------------------------------------------
def plot_proximity():
    df = load_metric("proximity")
    if df.empty:
        return

    df["neighbors"] = df["neighbors"].astype(int)

    plt.figure()
    sns.histplot(df["neighbors"], bins=40, kde=True)
    plt.title("Proximity: Neighbor Count Distribution")
    plt.xlabel("Neighbors")
    plt.ylabel("Count")

    out = OUT / "proximity"
    out.mkdir(exist_ok=True)
    plt.savefig(out / "proximity_neighbors_hist.png")
    plt.close()

# ---------------------------------------------------------
# 7.3.2 SPATIAL INDEX QUERY TIME
# ---------------------------------------------------------
def plot_spatial_query_time():
    df = load_metric("spatial_query")
    if df.empty:
        return

    df["timeMs"] = df["timeMs"].astype(float)

    plt.figure()
    sns.histplot(df["timeMs"], bins=50, kde=True)
    plt.title("Spatial Index Query Time Distribution")
    plt.xlabel("Query Time (ms)")
    plt.ylabel("Count")

    out = OUT / "spatial_index"
    out.mkdir(exist_ok=True)
    plt.savefig(out / "spatial_query_time_hist.png")
    plt.close()

# ---------------------------------------------------------
# 7.4 COLLISION
# ---------------------------------------------------------
def plot_collision():
    df = load_metric("collision")
    if df.empty:
        return

    df["collisions"] = df["collisions"].astype(int)

    plt.figure()
    sns.histplot(df["collisions"], bins=20, kde=False)
    plt.title("Collision Count Distribution")
    plt.xlabel("Collisions per Window")
    plt.ylabel("Count")

    out = OUT / "collision"
    out.mkdir(exist_ok=True)
    plt.savefig(out / "collision_hist.png")
    plt.close()

# ---------------------------------------------------------
# 7.5 CONFLICT
# ---------------------------------------------------------
def plot_conflict():
    df = load_metric("conflict")
    if df.empty:
        return

    df["conflicts"] = df["conflicts"].astype(int)

    plt.figure()
    sns.histplot(df["conflicts"], bins=20, kde=False)
    plt.title("Conflict Count Distribution")
    plt.xlabel("Conflicts per Window")
    plt.ylabel("Count")

    out = OUT / "conflict"
    out.mkdir(exist_ok=True)
    plt.savefig(out / "conflict_hist.png")
    plt.close()

# ---------------------------------------------------------
# 7.6 SWARM (density)
# ---------------------------------------------------------
def plot_swarm_density():
    df = load_metric("density")
    if df.empty:
        return

    df["density"] = df["density"].astype(float)

    plt.figure()
    sns.histplot(df["density"], bins=40, kde=True)
    plt.title("Swarm Density Distribution")
    plt.xlabel("Density")
    plt.ylabel("Count")

    out = OUT / "swarm"
    out.mkdir(exist_ok=True)
    plt.savefig(out / "swarm_density_hist.png")
    plt.close()

# ---------------------------------------------------------
# 7.7 TRAJECTORY
# ---------------------------------------------------------
def plot_trajectory_timegap():
    df = load_metric("trajectory_update")
    if df.empty:
        return

    df["timeGap"] = df["timeGap"].astype(float)

    plt.figure()
    sns.histplot(df["timeGap"], bins=50, kde=True)
    plt.title("Trajectory Time Gap Distribution")
    plt.xlabel("Time Gap (ms)")
    plt.ylabel("Count")

    out = OUT / "trajectory"
    out.mkdir(exist_ok=True)
    plt.savefig(out / "trajectory_timegap_hist.png")
    plt.close()

# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------
def main():
    plot_timestamp_lag()
    plot_window_latency()
    plot_proximity()
    plot_spatial_query_time()
    plot_collision()
    plot_conflict()
    plot_swarm_density()
    plot_trajectory_timegap()

    print("All visualizations generated.")

if __name__ == "__main__":
    main()
