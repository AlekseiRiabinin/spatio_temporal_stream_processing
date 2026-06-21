# %% [markdown]
# # Experiment 5.2 — Adaptive vs Static Watermark
#
# This notebook compares the adaptive watermark mechanism against
# several fixed watermark delays.
#
# Figures produced:
#
# - B1_completeness_comparison.png
# - B2_dropped_events_comparison.png
# - B3_watermark_lag_comparison.png
#
# Table produced:
#
# - watermark_statistics.csv

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
# ## Validation

# %%
required_columns = [
    "mode",
    "watermark_delay_ms",
    "adaptive_watermark_ms",
    "late_event_ratio",
    "watermark_lag_ms",
    "processing_latency_ms"
]

missing = [c for c in required_columns if c not in df.columns]

if missing:
    raise ValueError(f"Missing columns: {missing}")

print("Validation successful.")

# %% [markdown]
# ## Compute completeness
#
# Completeness = 1 − late_event_ratio

# %%
df["completeness"] = 1.0 - df["late_event_ratio"]

# %% [markdown]
# ## Keep Fixed and Adaptive runs

# %%
df = df[df["mode"].isin(["fixed", "adaptive"])].copy()

# %% [markdown]
# ## Fixed watermark statistics

# %%
fixed = df[df["mode"] == "fixed"].copy()

fixed_groups = (
    fixed
    .groupby("watermark_delay_ms")
    .agg(
        completeness_mean=("completeness", "mean"),
        completeness_std=("completeness", "std"),

        late_mean=("late_event_ratio", "mean"),
        late_std=("late_event_ratio", "std"),

        lag_mean=("watermark_lag_ms", "mean"),
        lag_std=("watermark_lag_ms", "std"),

        count=("completeness", "count")
    )
    .reset_index()
)

fixed_groups

# %% [markdown]
# ## Adaptive watermark statistics

# %%
adaptive = df[df["mode"] == "adaptive"].copy()

adaptive_stats = {
    "watermark_delay_ms": adaptive["adaptive_watermark_ms"].mean(),

    "completeness_mean": adaptive["completeness"].mean(),
    "completeness_std": adaptive["completeness"].std(),

    "late_mean": adaptive["late_event_ratio"].mean(),
    "late_std": adaptive["late_event_ratio"].std(),

    "lag_mean": adaptive["watermark_lag_ms"].mean(),
    "lag_std": adaptive["watermark_lag_ms"].std(),

    "count": len(adaptive)
}

adaptive_stats

# %% [markdown]
# ## Summary table

# %%
summary = fixed_groups.copy()

summary = pd.concat(
    [
        summary,
        pd.DataFrame([adaptive_stats])
    ],
    ignore_index=True
)

summary["configuration"] = summary["watermark_delay_ms"].apply(
    lambda x: f"Fixed {int(x)} ms"
)

summary.loc[
    summary.index[-1],
    "configuration"
] = f"Adaptive ({adaptive_stats['watermark_delay_ms']:.0f} ms)"

summary

# %% [markdown]
# ## Confidence intervals

# %%
for metric in [
    "completeness",
    "late",
    "lag"
]:

    summary[f"{metric}_ci95"] = (
        1.96
        * summary[f"{metric}_std"]
        / np.sqrt(summary["count"])
    )

summary

# %% [markdown]
# ## Figure B1
#
# Data completeness

# %%
plt.figure()

plt.bar(
    summary["configuration"],
    summary["completeness_mean"],
    yerr=summary["completeness_ci95"],
    capsize=5
)

plt.ylabel("Data completeness")
plt.xlabel("Watermark configuration")
plt.title("Adaptive vs Static Watermark")

plt.xticks(rotation=20)

plt.tight_layout()

outfile = FIGURE_DIR / "B1_completeness_comparison.png"

plt.savefig(outfile)

plt.show()

# %% [markdown]
# ## Figure B2
#
# Late event ratio

# %%
plt.figure()

plt.bar(
    summary["configuration"],
    summary["late_mean"],
    yerr=summary["late_ci95"],
    capsize=5
)

plt.ylabel("Late event ratio")
plt.xlabel("Watermark configuration")
plt.title("Dropped/Late Events")

plt.xticks(rotation=20)

plt.tight_layout()

outfile = FIGURE_DIR / "B2_dropped_events_comparison.png"

plt.savefig(outfile)

plt.show()

# %% [markdown]
# ## Figure B3
#
# Watermark lag distribution

# %%
labels = [
    f"Fixed {int(w)}"
    for w in sorted(fixed["watermark_delay_ms"].unique())
]

data = [
    fixed.loc[
        fixed["watermark_delay_ms"] == w,
        "watermark_lag_ms"
    ]
    for w in sorted(fixed["watermark_delay_ms"].unique())
]

labels.append("Adaptive")

data.append(
    adaptive["watermark_lag_ms"]
)

plt.figure()

plt.boxplot(
    data,
    labels=labels,
    showfliers=True
)

plt.ylabel("Watermark lag (ms)")
plt.xlabel("Configuration")
plt.title("Watermark Lag Distribution")

plt.tight_layout()

outfile = FIGURE_DIR / "B3_watermark_lag_comparison.png"

plt.savefig(outfile)

plt.show()

# %% [markdown]
# ## Export statistics

# %%
outfile = TABLE_DIR / "watermark_statistics.csv"

summary.to_csv(outfile, index=False)

print(outfile)

# %% [markdown]
# ## Numerical summary

# %%
pd.set_option("display.max_columns", None)

print(summary.round(3))
