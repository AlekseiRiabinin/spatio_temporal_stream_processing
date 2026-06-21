# %% [markdown]
# # Experiment 5.4 — Robustness to Disorder
#
# This notebook evaluates how the adaptive controller behaves
# under increasing event disorder.
#
# Figures produced:
#
# - D1_disorder_vs_latency.png
# - D2_disorder_vs_completeness.png
# - D3_disorder_vs_throughput.png
#
# Table produced:
#
# - disorder_statistics.csv

# %%
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

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
    "disorder_ratio",
    "avg_latency_ms",
    "event_rate",
    "late_event_ratio"
]

missing = [c for c in required if c not in df.columns]

if missing:
    raise ValueError(missing)

# %% [markdown]
# ## Derived metrics

# %%
df["completeness"] = 1.0 - df["late_event_ratio"]

df = df[df["mode"].isin(["fixed", "adaptive"])]

# %% [markdown]
# ## Disorder summary

# %%
summary = (
    df.groupby("mode")
      .agg(
          disorder_mean=("disorder_ratio","mean"),
          latency_mean=("avg_latency_ms","mean"),
          throughput_mean=("event_rate","mean"),
          completeness_mean=("completeness","mean"),
          count=("mode","count")
      )
      .reset_index()
)

summary

# %% [markdown]
# ## Figure D1
#
# Disorder vs latency

# %%
plt.figure()

for mode in ["fixed", "adaptive"]:

    subset = df[df["mode"] == mode]

    plt.scatter(
        subset["disorder_ratio"],
        subset["avg_latency_ms"],
        alpha=0.7,
        label=mode
    )

    z = np.polyfit(
        subset["disorder_ratio"],
        subset["avg_latency_ms"],
        1
    )

    x = np.linspace(
        subset["disorder_ratio"].min(),
        subset["disorder_ratio"].max(),
        100
    )

    plt.plot(
        x,
        np.polyval(z, x)
    )

plt.xlabel("Disorder ratio")
plt.ylabel("Average latency (ms)")
plt.title("Latency vs Event Disorder")

plt.legend()

plt.tight_layout()

outfile = FIGURE_DIR / "D1_disorder_vs_latency.png"

plt.savefig(outfile)

plt.show()

# %% [markdown]
# ## Figure D2
#
# Disorder vs completeness

# %%
plt.figure()

for mode in ["fixed", "adaptive"]:

    subset = df[df["mode"] == mode]

    plt.scatter(
        subset["disorder_ratio"],
        subset["completeness"],
        alpha=0.7,
        label=mode
    )

    z = np.polyfit(
        subset["disorder_ratio"],
        subset["completeness"],
        1
    )

    x = np.linspace(
        subset["disorder_ratio"].min(),
        subset["disorder_ratio"].max(),
        100
    )

    plt.plot(
        x,
        np.polyval(z, x)
    )

plt.xlabel("Disorder ratio")
plt.ylabel("Completeness")
plt.title("Completeness vs Event Disorder")

plt.legend()

plt.tight_layout()

outfile = FIGURE_DIR / "D2_disorder_vs_completeness.png"

plt.savefig(outfile)

plt.show()

# %% [markdown]
# ## Figure D3
#
# Disorder vs throughput

# %%
plt.figure()

for mode in ["fixed", "adaptive"]:

    subset = df[df["mode"] == mode]

    plt.scatter(
        subset["disorder_ratio"],
        subset["event_rate"],
        alpha=0.7,
        label=mode
    )

    z = np.polyfit(
        subset["disorder_ratio"],
        subset["event_rate"],
        1
    )

    x = np.linspace(
        subset["disorder_ratio"].min(),
        subset["disorder_ratio"].max(),
        100
    )

    plt.plot(
        x,
        np.polyval(z, x)
    )

plt.xlabel("Disorder ratio")
plt.ylabel("Throughput (events/sec)")
plt.title("Throughput vs Event Disorder")

plt.legend()

plt.tight_layout()

outfile = FIGURE_DIR / "D3_disorder_vs_throughput.png"

plt.savefig(outfile)

plt.show()

# %% [markdown]
# ## Correlation analysis

# %%
correlations = []

for mode in ["fixed", "adaptive"]:

    subset = df[df["mode"] == mode]

    correlations.append({
        "mode": mode,
        "latency_corr":
            subset["disorder_ratio"].corr(subset["avg_latency_ms"]),
        "throughput_corr":
            subset["disorder_ratio"].corr(subset["event_rate"]),
        "completeness_corr":
            subset["disorder_ratio"].corr(subset["completeness"])
    })

correlations = pd.DataFrame(correlations)

correlations

# %% [markdown]
# ## Export tables

# %%
summary.to_csv(
    TABLE_DIR / "disorder_statistics.csv",
    index=False
)

correlations.to_csv(
    TABLE_DIR / "disorder_correlations.csv",
    index=False
)

# %% [markdown]
# ## Numerical summary

# %%
pd.set_option("display.max_columns", None)

print(summary.round(3))
print()
print(correlations.round(3))
