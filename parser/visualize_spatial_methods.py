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

    out = OUT / "timestamp"
    out.mkdir(exist_ok=True)

    plt.figure(figsize=(10, 6))
    sns.boxplot(
        data=df,
        x="timestamp_pattern",
        y="lag",
        hue="timestamp_pattern",
        palette="Set2",
        legend=False
    )
    sns.stripplot(
        data=df,
        x="timestamp_pattern",
        y="lag",
        color="black",
        alpha=0.15,
        size=2
    )

    plt.title("Timestamp Lag Distribution by Timestamp Pattern")
    plt.xlabel("Timestamp Pattern")
    plt.ylabel("Lag (ms)")

    plt.tight_layout()
    plt.savefig(out / "timestamp_lag_boxplot.png", dpi=200)
    plt.savefig(out / "timestamp_lag_boxplot.pdf")
    plt.close()

# ---------------------------------------------------------
# 7.2.2 WINDOW LATENCY
# ---------------------------------------------------------
def plot_window_latency():
    df = load_metric("window")
    if df.empty:
        return

    df["latency"] = df["processingTime"] - df["windowEnd"]

    out = OUT / "window"
    out.mkdir(exist_ok=True)

    # --- Global histogram ---
    plt.figure(figsize=(10, 6))
    sns.histplot(df["latency"], bins=60, kde=True, color="steelblue")
    plt.title("Window Emission Latency Distribution")
    plt.xlabel("Latency (ms)")
    plt.ylabel("Count")
    plt.tight_layout()
    plt.savefig(out / "window_latency_hist.png", dpi=200)
    plt.savefig(out / "window_latency_hist.pdf")
    plt.close()

    # --- Faceted histogram by rate pattern ---
    g = sns.FacetGrid(df, col="rate_pattern", col_wrap=3, height=4, sharex=True, sharey=True)
    g.map_dataframe(sns.histplot, x="latency", bins=50, kde=True)
    g.set_axis_labels("Latency (ms)", "Count")
    g.fig.suptitle("Window Latency by Rate Pattern", y=1.03)

    g.savefig(out / "window_latency_by_rate_pattern.png", dpi=200)
    g.savefig(out / "window_latency_by_rate_pattern.pdf")
    plt.close()

# ---------------------------------------------------------
# 7.3.1 PROXIMITY
# ---------------------------------------------------------
def plot_proximity():
    df = load_metric("proximity_pair")
    if df.empty:
        return

    df["distance"] = df["distance"].astype(float)

    out = OUT / "proximity"
    out.mkdir(exist_ok=True)

    # --- Histogram + KDE faceted by motion_mode ---
    g = sns.FacetGrid(
        df,
        col="motion_mode",
        col_wrap=3,
        height=4,
        sharex=True,
        sharey=True
    )
    g.map_dataframe(sns.histplot, x="distance", bins=50, kde=True)
    g.set_axis_labels("Distance (m)", "Count")
    g.fig.suptitle("Proximity Distance Distribution by Motion Mode", y=1.03)

    g.fig.set_tight_layout(False)
    g.fig.set_size_inches(12, 8)

    g.savefig(out / "proximity_distance_by_motion_mode.png", dpi=200)
    g.savefig(out / "proximity_distance_by_motion_mode.pdf")
    plt.close()

# ---------------------------------------------------------
# 7.3.2 SPATIAL INDEX QUERY TIME
# ---------------------------------------------------------
def plot_spatial_query_time():
    df = load_metric("spatial_query")
    if df.empty:
        return

    df["timeMs"] = df["timeMs"].astype(float)
    df["candidates"] = df["candidates"].astype(int)

    out = OUT / "spatial_index"
    out.mkdir(exist_ok=True)

    # --- Histogram of query time ---
    plt.figure(figsize=(10, 6))
    sns.histplot(df["timeMs"], bins=60, kde=True, color="steelblue")
    plt.title("Spatial Index Query Time Distribution")
    plt.xlabel("Query Time (ms)")
    plt.ylabel("Count")
    plt.tight_layout()
    plt.savefig(out / "spatial_query_time_hist.png", dpi=200)
    plt.savefig(out / "spatial_query_time_hist.pdf")
    plt.close()

    # --- Scatter: candidates vs timeMs ---
    plt.figure(figsize=(10, 6))
    sns.scatterplot(
        data=df,
        x="candidates",
        y="timeMs",
        hue="geometry",
        alpha=0.6
    )
    plt.title("Spatial Index Scaling: Candidates vs Query Time")
    plt.xlabel("Candidates")
    plt.ylabel("Query Time (ms)")
    plt.tight_layout()
    plt.savefig(out / "spatial_query_candidates_vs_time.png", dpi=200)
    plt.savefig(out / "spatial_query_candidates_vs_time.pdf")
    plt.close()

    # --- Boxplot: timeMs by geometry ---
    plt.figure(figsize=(8, 6))
    sns.boxplot(
        data=df,
        x="geometry",
        y="timeMs",
        hue="geometry",
        palette="Set2",
        legend=False
    )
    plt.title("Spatial Index Query Time by Geometry")
    plt.xlabel("Geometry")
    plt.ylabel("Query Time (ms)")
    plt.tight_layout()
    plt.savefig(out / "spatial_query_time_by_geometry.png", dpi=200)
    plt.savefig(out / "spatial_query_time_by_geometry.pdf")
    plt.close()

# ---------------------------------------------------------
# 7.3.3 KNN
# ---------------------------------------------------------
def plot_knn_performance():
    df = load_metric("knn")
    if df.empty:
        return

    df["candidates"] = df["candidates"].astype(int)
    df["distanceComputations"] = df["distanceComputations"].astype(int)
    df["timeMs"] = df["timeMs"].astype(float)

    out = OUT / "knn"
    out.mkdir(exist_ok=True)

    # --- Scatter: candidates vs timeMs ---
    plt.figure(figsize=(10, 6))
    sns.scatterplot(
        data=df,
        x="candidates",
        y="timeMs",
        hue="geometry",
        alpha=0.6
    )
    plt.title("KNN Performance: Candidates vs Query Time")
    plt.xlabel("Candidates")
    plt.ylabel("Query Time (ms)")
    plt.tight_layout()
    plt.savefig(out / "knn_candidates_vs_time.png", dpi=200)
    plt.savefig(out / "knn_candidates_vs_time.pdf")
    plt.close()

    # --- Boxplot: timeMs by geometry ---
    plt.figure(figsize=(8, 6))
    sns.boxplot(
        data=df,
        x="geometry",
        y="timeMs",
        hue="geometry",
        palette="Set2",
        legend=False
    )
    plt.title("KNN Query Time by Geometry")
    plt.xlabel("Geometry")
    plt.ylabel("Query Time (ms)")
    plt.tight_layout()
    plt.savefig(out / "knn_time_by_geometry.png", dpi=200)
    plt.savefig(out / "knn_time_by_geometry.pdf")
    plt.close()

    # --- Histogram: distanceComputations ---
    plt.figure(figsize=(10, 6))
    sns.histplot(df["distanceComputations"], bins=50, kde=True)
    plt.title("KNN Distance Computations Distribution")
    plt.xlabel("Distance Computations")
    plt.ylabel("Count")
    plt.tight_layout()
    plt.savefig(out / "knn_distance_computations_hist.png", dpi=200)
    plt.savefig(out / "knn_distance_computations_hist.pdf")
    plt.close()

# ---------------------------------------------------------
# 7.4 COLLISION
# ---------------------------------------------------------
def plot_collision_analysis():
    # Try primary metric
    df = load_metric("collision_pair")

    # Fallback: use collision_detected if collision_pair is empty
    if df.empty:
        df = load_metric("collision_detected")
        if df.empty:
            print("No collision_pair or collision_detected data available.")
            return

        # collision_detected has no relativeSpeed → add placeholder
        df["relativeSpeed"] = float("nan")

    # Convert Arrow-backed columns to NumPy for speed
    df = df.copy()
    df["ttc"] = df["ttc"].astype(float)
    df["distance"] = df["distance"].astype(float)
    df["relativeSpeed"] = df["relativeSpeed"].astype(float)
    df["motion_mode"] = df["motion_mode"].astype(str)

    out = OUT / "collision"
    out.mkdir(exist_ok=True)

    # -----------------------------------------------------
    # Visualization 1 — TTC Distribution (Histogram + KDE)
    # -----------------------------------------------------
    g = sns.FacetGrid(
        df,
        col="motion_mode",
        col_wrap=3,
        height=4,
        sharex=True,
        sharey=True
    )
    g.map_dataframe(sns.histplot, x="ttc", bins=50, kde=True)
    g.set_axis_labels("TTC (s)", "Count")
    g.fig.suptitle("TTC Distribution by Motion Mode", y=1.03)

    # FIX: prevent Matplotlib freeze
    g.fig.set_tight_layout(False)
    g.fig.set_size_inches(12, 8)

    g.savefig(out / "ttc_distribution_by_motion_mode.png", dpi=200)
    g.savefig(out / "ttc_distribution_by_motion_mode.pdf")
    plt.close()

    # -----------------------------------------------------
    # Visualization 2 — TTC vs Distance (Scatter Plot)
    # -----------------------------------------------------
    plt.figure(figsize=(10, 6))
    sns.scatterplot(
        data=df,
        x="distance",
        y="ttc",
        hue="motion_mode",
        alpha=0.6
    )
    plt.title("TTC vs Distance")
    plt.xlabel("Distance (m)")
    plt.ylabel("TTC (s)")
    plt.yscale("log")
    plt.tight_layout()

    plt.savefig(out / "ttc_vs_distance.png", dpi=200)
    plt.savefig(out / "ttc_vs_distance.pdf")
    plt.close()

    # -----------------------------------------------------
    # Visualization 3 — Relative Speed Distribution
    # -----------------------------------------------------
    plt.figure(figsize=(10, 6))
    sns.histplot(df["relativeSpeed"], bins=50, kde=True, color="darkred")
    plt.title("Relative Speed Distribution")
    plt.xlabel("Relative Speed (m/s)")
    plt.ylabel("Count")
    plt.tight_layout()

    plt.savefig(out / "relative_speed_hist.png", dpi=200)
    plt.savefig(out / "relative_speed_hist.pdf")
    plt.close()

# ---------------------------------------------------------
# 7.5 CONFLICT
# ---------------------------------------------------------
def plot_conflict_analysis():
    # ---------- Pair-level conflicts ----------
    df_pair = load_metric("conflict_pair")
    df_win = load_metric("conflict")

    out = OUT / "conflict"
    out.mkdir(exist_ok=True)

    # If we have pair-level data, do distance + horizon plots
    if not df_pair.empty:
        df_pair["distance"] = df_pair["distance"].astype(float)
        df_pair["t"] = df_pair["t"].astype(float)

        # -------------------------------------------------
        # Visualization 1 — Distribution of Predicted Distances
        # -------------------------------------------------
        g = sns.FacetGrid(
            df_pair,
            col="motion_mode",
            col_wrap=3,
            height=4,
            sharex=True,
            sharey=True
        )
        g.map_dataframe(sns.histplot, x="distance", bins=50, kde=True)
        g.set_axis_labels("Predicted Minimum Distance (m)", "Count")
        g.fig.suptitle("Predicted Conflict Distance Distribution by Motion Mode", y=1.03)

        g.savefig(out / "conflict_predicted_distance_by_motion_mode.png", dpi=200)
        g.savefig(out / "conflict_predicted_distance_by_motion_mode.pdf")
        plt.close()

        # -------------------------------------------------
        # Visualization 2 — Conflict Time Horizon (t vs distance)
        # -------------------------------------------------
        plt.figure(figsize=(10, 6))
        sns.scatterplot(
            data=df_pair,
            x="t",
            y="distance",
            hue="motion_mode",
            alpha=0.6
        )
        plt.title("Conflict Prediction Horizon vs Predicted Distance")
        plt.xlabel("Prediction Horizon t (s)")
        plt.ylabel("Predicted Minimum Distance (m)")
        plt.tight_layout()

        plt.savefig(out / "conflict_time_horizon_vs_distance.png", dpi=200)
        plt.savefig(out / "conflict_time_horizon_vs_distance.pdf")
        plt.close()

    # -------------------------------------------------
    # Visualization 3 — Conflict Count per Window
    # -------------------------------------------------
    if not df_win.empty:
        df_win["conflicts"] = df_win["conflicts"].astype(int)

        plt.figure(figsize=(8, 6))
        sns.boxplot(
            data=df_win,
            x="motion_mode",
            y="conflicts",
            hue="motion_mode",
            palette="Set2",
            legend=False
        )
        plt.title("Conflicts per Window by Motion Mode")
        plt.xlabel("Motion Mode")
        plt.ylabel("Conflicts per Window")
        plt.tight_layout()

        plt.savefig(out / "conflict_count_by_motion_mode.png", dpi=200)
        plt.savefig(out / "conflict_count_by_motion_mode.pdf")
        plt.close()

# ---------------------------------------------------------
# 7.6 SWARM (ST‑DBSCAN clustering)
# ---------------------------------------------------------
def plot_swarm_analysis():
    df_seed = load_metric("swarm_seed")
    df_expand = load_metric("swarm_expand")
    df_index = load_metric("swarm_spatial_index_built")

    out = OUT / "swarm"
    out.mkdir(exist_ok=True)

    # -----------------------------------------------------
    # Visualization 1 — Cluster Size Distribution
    # -----------------------------------------------------
    if not df_seed.empty and not df_expand.empty:
        # Convert timestamps for temporal analysis
        df_seed["timestamp"] = pd.to_datetime(
            df_seed["date"].astype(str) + df_seed["time"].astype(str),
            format="%Y%m%d%H%M%S"
        )
        df_expand["timestamp"] = pd.to_datetime(
            df_expand["date"].astype(str) + df_expand["time"].astype(str),
            format="%Y%m%d%H%M%S"
        )

        # Count expand events per seed (cluster)
        expand_counts = df_expand.groupby("objectId").size()
        seed_ids = df_seed["objectId"].unique()

        cluster_sizes = []
        for obj in seed_ids:
            size = 1 + expand_counts.get(obj, 0)
            cluster_sizes.append(size)

        df_sizes = pd.DataFrame({"cluster_size": cluster_sizes})

        plt.figure(figsize=(10, 6))
        sns.histplot(df_sizes["cluster_size"], bins=30, kde=True, color="steelblue")
        plt.title("Cluster Size Distribution (ST-DBSCAN)")
        plt.xlabel("Cluster Size")
        plt.ylabel("Frequency")
        plt.tight_layout()

        plt.savefig(out / "swarm_cluster_size_hist.png", dpi=200)
        plt.savefig(out / "swarm_cluster_size_hist.pdf")
        plt.close()

    # -----------------------------------------------------
    # Visualization 2 — Temporal Dynamics of Clustering
    # -----------------------------------------------------
    if not df_expand.empty:
        df_expand["timestamp"] = pd.to_datetime(
            df_expand["date"].astype(str) + df_expand["time"].astype(str),
            format="%Y%m%d%H%M%S"
        )

        # Count expand events per time window
        df_expand["count"] = 1
        df_time = df_expand.groupby("timestamp")["count"].sum().reset_index()

        plt.figure(figsize=(12, 6))
        sns.lineplot(data=df_time, x="timestamp", y="count", color="darkgreen")
        plt.title("Temporal Dynamics of Cluster Growth")
        plt.xlabel("Time")
        plt.ylabel("Expand Events per Timestamp")
        plt.tight_layout()

        plt.savefig(out / "swarm_temporal_dynamics.png", dpi=200)
        plt.savefig(out / "swarm_temporal_dynamics.pdf")
        plt.close()

    # -----------------------------------------------------
    # Visualization 3 — Spatial Index Load During Clustering
    # -----------------------------------------------------
    if not df_index.empty:
        df_index["size"] = df_index["size"].astype(int)

        plt.figure(figsize=(8, 6))
        sns.boxplot(
            data=df_index,
            x="geometry",
            y="size",
            hue="geometry",
            palette="Set2",
            legend=False
        )
        plt.title("Spatial Index Size During Swarm Clustering")
        plt.xlabel("Geometry")
        plt.ylabel("Index Size")
        plt.tight_layout()

        plt.savefig(out / "swarm_spatial_index_size_by_geometry.png", dpi=200)
        plt.savefig(out / "swarm_spatial_index_size_by_geometry.pdf")
        plt.close()

# ---------------------------------------------------------
# 7.7 TRAJECTORY & DENSITY
# ---------------------------------------------------------

def plot_trajectory_and_density():
    df_traj = load_metric("trajectory_update")
    df_density = load_metric("density")
    df_spatial = load_metric("spatial_insert")  # optional for heatmap

    out = OUT / "trajectory_density"
    out.mkdir(exist_ok=True)

    # -----------------------------------------------------
    # Visualization 1 — Trajectory Segment Length Distribution
    # -----------------------------------------------------
    if not df_traj.empty:
        df_traj["length"] = df_traj["length"].astype(float)

        g = sns.FacetGrid(
            df_traj,
            col="motion_mode",
            col_wrap=3,
            height=4,
            sharex=True,
            sharey=True
        )
        g.map_dataframe(sns.histplot, x="length", bins=50, kde=True)
        g.set_axis_labels("Segment Length (m)", "Count")
        g.fig.suptitle("Trajectory Segment Length Distribution by Motion Mode", y=1.03)

        g.savefig(out / "trajectory_segment_length_by_motion_mode.png", dpi=200)
        g.savefig(out / "trajectory_segment_length_by_motion_mode.pdf")
        plt.close()

    # -----------------------------------------------------
    # Visualization 2 — Speed Distribution (Boxplot)
    # -----------------------------------------------------
    if not df_traj.empty:
        df_traj["length"] = df_traj["length"].astype(float)
        df_traj["timeGap"] = df_traj["timeGap"].astype(float)

        # speed = length / (timeGap / 1000)
        df_traj["speed"] = df_traj["length"] / (df_traj["timeGap"] / 1000.0)

        plt.figure(figsize=(10, 6))
        sns.boxplot(
            data=df_traj,
            x="motion_mode",
            y="speed",
            hue="motion_mode",
            palette="Set2",
            legend=False
        )
        plt.title("Speed Distribution by Motion Mode")
        plt.xlabel("Motion Mode")
        plt.ylabel("Speed (m/s)")
        plt.tight_layout()

        plt.savefig(out / "trajectory_speed_by_motion_mode.png", dpi=200)
        plt.savefig(out / "trajectory_speed_by_motion_mode.pdf")
        plt.close()

    # -----------------------------------------------------
    # Visualization 3 — Direction Change Distribution
    # -----------------------------------------------------
    # NOTE: Requires reconstructing direction vectors from consecutive segments.
    # If spatial coordinates are not available, we approximate direction change
    # using timeGap and segment length (not ideal but consistent).
    if not df_traj.empty:
        # Sort by object and timestamp
        df_traj["timestamp"] = pd.to_datetime(
            df_traj["date"].astype(str) + df_traj["time"].astype(str),
            format="%Y%m%d%H%M%S"
        )
        df_traj = df_traj.sort_values(["objectId", "timestamp"])

        # Approximate direction change using differences in segment length
        df_traj["prev_length"] = df_traj.groupby("objectId")["length"].shift(1)
        df_traj["direction_change"] = (df_traj["length"] - df_traj["prev_length"]).abs()

        plt.figure(figsize=(10, 6))
        sns.histplot(df_traj["direction_change"].dropna(), bins=50, kde=True)
        plt.title("Direction Change Distribution (Approx.)")
        plt.xlabel("Direction Change (Δ length)")
        plt.ylabel("Count")
        plt.tight_layout()

        plt.savefig(out / "trajectory_direction_change_hist.png", dpi=200)
        plt.savefig(out / "trajectory_direction_change_hist.pdf")
        plt.close()

    # -----------------------------------------------------
    # Visualization 4 — Density Heatmap
    # -----------------------------------------------------
    if not df_density.empty and not df_spatial.empty:
        df_spatial["lat"] = df_spatial["lat"].astype(float)
        df_spatial["lon"] = df_spatial["lon"].astype(float)

        plt.figure(figsize=(10, 8))
        sns.kdeplot(
            x=df_spatial["lon"],
            y=df_spatial["lat"],
            fill=True,
            cmap="viridis",
            bw_adjust=0.5,
            thresh=0.05
        )
        plt.title("Spatial Density Heatmap")
        plt.xlabel("Longitude")
        plt.ylabel("Latitude")
        plt.tight_layout()

        plt.savefig(out / "density_heatmap.png", dpi=200)
        plt.savefig(out / "density_heatmap.pdf")
        plt.close()

    # -----------------------------------------------------
    # Visualization 5 — Density Time Series
    # -----------------------------------------------------
    if not df_density.empty:
        df_density["timestamp"] = pd.to_datetime(
            df_density["date"].astype(str) + df_density["time"].astype(str),
            format="%Y%m%d%H%M%S"
        )
        df_density["density"] = df_density["density"].astype(float)

        plt.figure(figsize=(12, 6))
        sns.lineplot(
            data=df_density,
            x="timestamp",
            y="density",
            hue="geometry"
        )
        plt.title("Global Density Over Time")
        plt.xlabel("Time")
        plt.ylabel("Density (objects/km²)")
        plt.tight_layout()

        plt.savefig(out / "density_time_series.png", dpi=200)
        plt.savefig(out / "density_time_series.pdf")
        plt.close()

# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------
def main():
    # 1. Input Stream Characterization
    plot_timestamp_lag()
    plot_window_latency()

    # 2. Spatial Analysis
    plot_proximity()
    plot_spatial_query_time()
    plot_knn_performance()

    # 3. Collision Analysis
    plot_collision_analysis()

    # 4. Conflict Prediction
    plot_conflict_analysis()

    # 5. Swarm Behavior Analysis
    plot_swarm_analysis()

    # 6. Trajectory & Density Analysis
    plot_trajectory_and_density()

    print("All visualizations generated.")


if __name__ == "__main__":
    main()
