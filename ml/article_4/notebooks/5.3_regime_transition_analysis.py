# %% [markdown]
# # Experiment 5.3 — Regime Transition Analysis
#
# This notebook evaluates how the adaptive controller responds
# to changing stream conditions.
#
# Figures produced:
#
# - C1_adaptive_window_timeseries.png
# - C2_adaptive_watermark_timeseries.png
# - C3_adaptation_response_time.png
#
# Table produced:
#
# - adaptation_statistics.csv

# %%
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

plt.rcParams["figure.figsize"] = (12, 6)
plt.rcParams["figure.dpi"] = 150
plt.rcParams["axes.grid"] = True

# %% [markdown]
# ## Configuration

# %%
ROOT = Path("../..").resolve()

DATA_FILE = ROOT / "data" / "article_4" / "parsed" / "all_experiments.csv"

FIGURE_DIR = ROOT / "ml" / "article_4" / "figures"
TABLE_DIR = ROOT / "ml" / "article_4" / "tables"

FIGURE_DIR.mkdir(parents=True, exist_ok=True)
TABLE_DIR.mkdir(parents=True, exist_ok=True)

# %% [markdown]
# ## Load dataset

# %%
df = pd.read_csv(DATA_FILE)

print(df.shape)

# %% [markdown]
# ## Validation

# %%
required = [
    "mode",
    "profile",
    "run_timestamp",
    "adaptive_window_ms",
    "adaptive_watermark_ms",
    "adaptation_count",
    "window_change_rate",
    "watermark_change_rate",
    "window_oscillation_rate",
    "watermark_oscillation_rate",
    "avg_adaptation_interval_ms"
]

missing = [c for c in required if c not in df.columns]

if missing:
    raise ValueError(missing)

# %% [markdown]
# ## Keep only adaptive runs

# %%
adaptive = df[df["mode"] == "adaptive"].copy()

adaptive["run_timestamp"] = pd.to_datetime(
    adaptive["run_timestamp"],
    format="%Y%m%d_%H%M%S"
)

adaptive = adaptive.sort_values("run_timestamp")

adaptive.head()

# %% [markdown]
# ## Compute elapsed experiment time

# %%
adaptive["elapsed_sec"] = (
    adaptive["run_timestamp"]
    - adaptive["run_timestamp"].min()
).dt.total_seconds()

# %% [markdown]
# ## Figure C1
#
# Adaptive window evolution

# %%
plt.figure()

for profile in adaptive["profile"].unique():

    subset = adaptive[
        adaptive["profile"] == profile
    ]

    plt.plot(
        subset["elapsed_sec"],
        subset["adaptive_window_ms"],
        marker="o",
        label=profile
    )

plt.xlabel("Elapsed time (seconds)")
plt.ylabel("Adaptive window (ms)")
plt.title("Adaptive Window Evolution")

plt.legend()

plt.tight_layout()

outfile = FIGURE_DIR / "C1_adaptive_window_timeseries.png"

plt.savefig(outfile)

plt.show()

# %% [markdown]
# ## Figure C2
#
# Adaptive watermark evolution

# %%
plt.figure()

for profile in adaptive["profile"].unique():

    subset = adaptive[
        adaptive["profile"] == profile
    ]

    plt.plot(
        subset["elapsed_sec"],
        subset["adaptive_watermark_ms"],
        marker="o",
        label=profile
    )

plt.xlabel("Elapsed time (seconds)")
plt.ylabel("Adaptive watermark (ms)")
plt.title("Adaptive Watermark Evolution")

plt.legend()

plt.tight_layout()

outfile = FIGURE_DIR / "C2_adaptive_watermark_timeseries.png"

plt.savefig(outfile)

plt.show()

# %% [markdown]
# ## Compute adaptation statistics

# %%
summary = (
    adaptive
    .groupby("profile")
    .agg(
        adaptations=("adaptation_count","mean"),
        adaptation_interval=("avg_adaptation_interval_ms","mean"),
        window_change_rate=("window_change_rate","mean"),
        watermark_change_rate=("watermark_change_rate","mean"),
        window_oscillation=("window_oscillation_rate","mean"),
        watermark_oscillation=("watermark_oscillation_rate","mean")
    )
    .reset_index()
)

summary

# %% [markdown]
# ## Figure C3
#
# Average adaptation interval

# %%
plt.figure()

plt.bar(
    summary["profile"],
    summary["adaptation_interval"]
)

plt.ylabel("Average adaptation interval (ms)")
plt.xlabel("Profile")
plt.title("Controller Stabilization Time")

plt.tight_layout()

outfile = FIGURE_DIR / "C3_adaptation_response_time.png"

plt.savefig(outfile)

plt.show()

# %% [markdown]
# ## Export statistics

# %%
outfile = TABLE_DIR / "adaptation_statistics.csv"

summary.to_csv(outfile, index=False)

print(outfile)

# %% [markdown]
# ## Numerical summary

# %%
pd.set_option("display.max_columns", None)

print(summary.round(3))
