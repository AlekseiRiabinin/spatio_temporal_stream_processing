import seaborn as sns
import matplotlib.pyplot as plt


def apply_style():
    sns.set_theme(
        context="paper",
        style="whitegrid",
        palette="deep",
        font_scale=1.2
    )
    plt.rcParams["figure.figsize"] = (10, 6)
    plt.rcParams["savefig.dpi"] = 300
    plt.rcParams["axes.spines.top"] = False
    plt.rcParams["axes.spines.right"] = False
