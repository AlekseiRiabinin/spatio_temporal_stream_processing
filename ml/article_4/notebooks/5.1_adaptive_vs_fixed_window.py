# %% [markdown]
# # Experiment 5.1 — Adaptive vs Fixed Window
#
# This notebook compares the adaptive window mechanism with several
# fixed window configurations.
#
# Figures produced:
#
# - A1_latency_comparison.png
# - A2_throughput_comparison.png
# - A3_window_fill_comparison.png
#
# Table produced:
#
# - window_statistics.csv

# %%
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# ----------------------------------------
# Matplotlib configuration
# ----------------------------------------

plt.rcParams["figure.figsize"] = (10, 6)
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

print(DATA_FILE)

# %% [markdown]
# ## Load dataset

# %%
df = pd.read_csv(DATA_FILE)

print(df.shape)
df.head()

# %% [markdown]
# ## Basic validation

# %%
required_columns = [
    "mode",
    "window_size_ms",
    "adaptive_window_ms",
    "avg_latency_ms",
    "event_rate",
    "window_fill_ratio"
]

missing = [c for c in required_columns if c not in df.columns]

if missing:
    raise ValueError(f"Missing columns: {missing}")

print("Validation successful.")

# %% [markdown]
# ## Keep only Fixed and Adaptive runs

# %%
df = df[df["mode"].isin(["fixed", "adaptive"])].copy()

print(df["mode"].value_counts())

# %% [markdown]
# ## Prepare fixed window statistics

# %%
fixed = df[df["mode"] == "fixed"].copy()

fixed_groups = (
    fixed
    .groupby("window_size_ms")
    .agg(
        latency_mean=("avg_latency_ms", "mean"),
        latency_std=("avg_latency_ms", "std"),
        throughput_mean=("event_rate", "mean"),
        throughput_std=("event_rate", "std"),
        fill_mean=("window_fill_ratio", "mean"),
        fill_std=("window_fill_ratio", "std"),
        count=("avg_latency_ms", "count")
    )
    .reset_index()
)

fixed_groups

# %% [markdown]
# ## Prepare adaptive statistics

# %%
adaptive = df[df["mode"] == "adaptive"].copy()

adaptive_stats = {
    "window_size_ms": adaptive["adaptive_window_ms"].mean(),
    "latency_mean": adaptive["avg_latency_ms"].mean(),
    "latency_std": adaptive["avg_latency_ms"].std(),
    "throughput_mean": adaptive["event_rate"].mean(),
    "throughput_std": adaptive["event_rate"].std(),
    "fill_mean": adaptive["window_fill_ratio"].mean(),
    "fill_std": adaptive["window_fill_ratio"].std(),
    "count": len(adaptive)
}

adaptive_stats

# %% [markdown]
# ## Build summary table

# %%
summary = fixed_groups.copy()

adaptive_row = pd.DataFrame([adaptive_stats])

summary = pd.concat([summary, adaptive_row], ignore_index=True)

summary["configuration"] = summary["window_size_ms"].apply(
    lambda x: f"Fixed {int(x)} ms"
)

summary.loc[
    summary.index[-1],
    "configuration"
] = f"Adaptive ({adaptive_stats['window_size_ms']:.0f} ms)"

summary

# %% [markdown]
# ## Compute 95% confidence intervals

# %%
summary["latency_ci95"] = (
    1.96
    * summary["latency_std"]
    / np.sqrt(summary["count"])
)

summary["throughput_ci95"] = (
    1.96
    * summary["throughput_std"]
    / np.sqrt(summary["count"])
)

summary["fill_ci95"] = (
    1.96
    * summary["fill_std"]
    / np.sqrt(summary["count"])
)

summary

# %% [markdown]
# ## Figure A1 — Latency comparison

# %%
plt.figure()

plt.bar(
    summary["configuration"],
    summary["latency_mean"],
    yerr=summary["latency_ci95"],
    capsize=5
)

plt.ylabel("Average latency (ms)")
plt.xlabel("Window configuration")
plt.title("Adaptive vs Fixed Window: Latency")

plt.xticks(rotation=20)

plt.tight_layout()

outfile = FIGURE_DIR / "A1_latency_comparison.png"

plt.savefig(outfile)

plt.show()

print(outfile)

# %% [markdown]
# ## Figure A2 — Throughput comparison

# %%
plt.figure()

plt.bar(
    summary["configuration"],
    summary["throughput_mean"],
    yerr=summary["throughput_ci95"],
    capsize=5
)

plt.ylabel("Throughput (events/sec)")
plt.xlabel("Window configuration")
plt.title("Adaptive vs Fixed Window: Throughput")

plt.xticks(rotation=20)

plt.tight_layout()

outfile = FIGURE_DIR / "A2_throughput_comparison.png"

plt.savefig(outfile)

plt.show()

print(outfile)

# %% [markdown]
# ## Figure A3 — Window fill ratio

# %%
labels = [
    f"Fixed {int(w)}"
    for w in sorted(fixed["window_size_ms"].unique())
]

data = [
    fixed.loc[
        fixed["window_size_ms"] == w,
        "window_fill_ratio"
    ]
    for w in sorted(fixed["window_size_ms"].unique())
]

labels.append("Adaptive")

data.append(
    adaptive["window_fill_ratio"]
)

plt.figure()

plt.boxplot(
    data,
    labels=labels,
    showfliers=True
)

plt.ylabel("Window fill ratio")
plt.xlabel("Configuration")
plt.title("Adaptive vs Fixed Window: Fill Ratio")

plt.tight_layout()

outfile = FIGURE_DIR / "A3_window_fill_comparison.png"

plt.savefig(outfile)

plt.show()

print(outfile)

# %% [markdown]
# ## Export summary table

# %%
outfile = TABLE_DIR / "window_statistics.csv"

summary.to_csv(outfile, index=False)

print(outfile)

# %% [markdown]
# ## Print numerical summary

# %%
pd.set_option("display.max_columns", None)

print(summary.round(3))
