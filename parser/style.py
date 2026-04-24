import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd


def apply_style():
    # Global Seaborn theme
    sns.set_theme(
        context="paper",
        style="whitegrid",
        palette="deep",
        font_scale=1.3
    )

    # Global figure settings
    plt.rcParams["figure.figsize"] = (10, 6)
    plt.rcParams["savefig.dpi"] = 300
    plt.rcParams["axes.spines.top"] = False
    plt.rcParams["axes.spines.right"] = False

    # Better color cycle for non-hue plots
    plt.rcParams["axes.prop_cycle"] = plt.cycler(
        color=sns.color_palette("deep")
    )

    # Improve layout for faceted plots
    plt.rcParams["figure.autolayout"] = True

    # Make gridlines subtle
    plt.rcParams["grid.alpha"] = 0.3
    plt.rcParams["grid.linestyle"] = "--"

    # Avoid Seaborn palette warnings by default
    sns.set_palette("deep")
