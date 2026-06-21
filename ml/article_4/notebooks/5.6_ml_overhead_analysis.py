# ============================================================
# Notebook: 06_ml_overhead_analysis.ipynb
#
# Figures:
#   F1 inference_latency.png
#   F2 control_loop_overhead.png
#   F3 inference_vs_window_size.png
#
# Section 5.6
# ============================================================

# ============================================================
# CELL 1
# Imports
# ============================================================

from pathlib import Path

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

plt.rcParams["figure.figsize"] = (9, 6)
plt.rcParams["font.size"] = 12


# ============================================================
# CELL 2
# Load dataset
# ============================================================

ROOT = Path("../..")

DATA = ROOT / "data/article_4/parsed/all_experiments.csv"
OUT = ROOT / "paper/article_4/figures"

OUT.mkdir(parents=True, exist_ok=True)

df = pd.read_csv(DATA)

print(df.shape)


# ============================================================
# CELL 3
# Keep only completed experiments
# ============================================================

df = df[df["mode"].isin(["fixed", "adaptive"])].copy()

adaptive = df[df["mode"] == "adaptive"].copy()
fixed = df[df["mode"] == "fixed"].copy()

print("Adaptive experiments:", len(adaptive))
print("Fixed experiments:", len(fixed))


# ============================================================
# CELL 4
# Summary statistics
# ============================================================

summary = adaptive[
    [
        "ml_inference_ms",
        "processing_latency_ms",
        "adaptive_window_ms",
        "adaptive_watermark_ms",
    ]
].describe()

print(summary)


# ============================================================
# CELL 5
# Figure F1
# Distribution of ML inference latency
# ============================================================

fig, ax = plt.subplots()

ax.hist(
    adaptive["ml_inference_ms"].dropna(),
    bins=20
)

ax.set_xlabel("ML inference (ms)")
ax.set_ylabel("Frequency")
ax.set_title("Distribution of ML Inference Latency")

plt.tight_layout()

plt.savefig(
    OUT / "inference_latency.png",
    dpi=300
)

plt.show()


# ============================================================
# CELL 6
# Figure F2
# Processing latency breakdown
# ============================================================

means = pd.DataFrame({
    "Component": [
        "ML inference",
        "Processing latency"
    ],
    "Latency (ms)": [
        adaptive["ml_inference_ms"].mean(),
        adaptive["processing_latency_ms"].mean()
    ]
})

fig, ax = plt.subplots()

ax.bar(
    means["Component"],
    means["Latency (ms)"]
)

ax.set_ylabel("Average latency (ms)")
ax.set_title("Control Loop Overhead")

plt.tight_layout()

plt.savefig(
    OUT / "control_loop_overhead.png",
    dpi=300
)

plt.show()


# ============================================================
# CELL 7
# Figure F3
# ML inference vs adaptive window
# ============================================================

fig, ax = plt.subplots()

ax.scatter(
    adaptive["adaptive_window_ms"],
    adaptive["ml_inference_ms"],
    alpha=0.7
)

ax.set_xlabel("Adaptive window (ms)")
ax.set_ylabel("ML inference (ms)")
ax.set_title("Inference Latency vs Adaptive Window Size")

plt.tight_layout()

plt.savefig(
    OUT / "inference_vs_window_size.png",
    dpi=300
)

plt.show()


# ============================================================
# CELL 8
# Inference grouped by stream profile
# ============================================================

profile = (
    adaptive.groupby("profile")["ml_inference_ms"]
    .agg(["mean", "std", "min", "max"])
    .round(2)
)

print(profile)


# ============================================================
# CELL 9
# Inference grouped by motion model
# ============================================================

motion = (
    adaptive.groupby("motion_mode")["ml_inference_ms"]
    .agg(["mean", "std", "min", "max"])
    .round(2)
)

print(motion)


# ============================================================
# CELL 10
# Overhead percentage
# ============================================================

adaptive["overhead_percent"] = (
    adaptive["ml_inference_ms"]
    / adaptive["processing_latency_ms"]
    * 100
)

stats = adaptive["overhead_percent"].describe()

print(stats)


# ============================================================
# CELL 11
# Overhead distribution
# ============================================================

fig, ax = plt.subplots()

ax.hist(
    adaptive["overhead_percent"],
    bins=20
)

ax.set_xlabel("ML overhead (%)")
ax.set_ylabel("Frequency")
ax.set_title("Distribution of ML Control Overhead")

plt.tight_layout()

plt.show()


# ============================================================
# CELL 12
# Warm-up analysis
# ============================================================

adaptive_sorted = adaptive.sort_values("run_timestamp")

fig, ax = plt.subplots()

ax.plot(
    np.arange(len(adaptive_sorted)),
    adaptive_sorted["ml_inference_ms"]
)

ax.set_xlabel("Experiment")
ax.set_ylabel("Inference (ms)")
ax.set_title("ML Inference Across Experiments")

plt.tight_layout()

plt.show()


# ============================================================
# CELL 13
# Numerical summary
# ============================================================

print()

print("Average inference time:")
print(adaptive["ml_inference_ms"].mean())

print()

print("Median inference time:")
print(adaptive["ml_inference_ms"].median())

print()

print("Maximum inference:")
print(adaptive["ml_inference_ms"].max())

print()

print("Minimum inference:")
print(adaptive["ml_inference_ms"].min())

print()

print("Average processing latency:")
print(adaptive["processing_latency_ms"].mean())

print()

print("Average ML overhead (%):")
print(adaptive["overhead_percent"].mean())


# ============================================================
# CELL 14
# Conclusions
# ============================================================

print("""
Section 5.6 Summary

The adaptive controller introduces only a small
computational overhead.

Main observations:

• ML inference is only a few milliseconds.

• Inference time is nearly independent of
  adaptive window size.

• ML execution represents only a small fraction
  of total processing latency.

• The adaptive controller therefore scales well
  and does not become the bottleneck of the
  streaming pipeline.

These results demonstrate that online machine
learning can be integrated into real-time
stream processing with negligible runtime cost.
""")
