import json
from math import ceil

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.backends.backend_pdf import PdfPages

plt.style.use("ggplot")


def load_json(filepath):
    with open(filepath) as file:
        return json.load(file)


def parse(string):
    return [float(value) for value in string[1:-1].split(" ") if value != ""]


def plot_ci(ax, molecule_name, intensity, upper_bound, lower_bound, mols_lags):
    x = np.arange(mols_lags + 1)
    ax.step(x, np.exp(intensity), where="mid")
    ax.fill_between(x, np.exp(lower_bound), np.exp(upper_bound), alpha=0.5, step="mid")
    ax.hlines(1, 0, max(x))
    ax.set_title(molecule_name)
    ax.set_ylim(0, 3)
    return ax


def plot_intensities(df: pd.DataFrame, mols_lags, n_cols=3):
    fig, axes = plt.subplots(
        ceil(len(df) / n_cols), n_cols, sharey=False, figsize=(16, 82)
    )
    for (i, row) in enumerate(df.iterrows()):
        plot_ci(
            axes[i // n_cols][i % n_cols],
            row[0],
            parse(row[1].refit_coeffs),
            parse(row[1].upper_bound),
            parse(row[1].lower_bound),
            mols_lags,
        )
    plt.tight_layout()
    return fig, axes


if __name__ == "__main__":
    cv_track = load_json("cv_track.json")
    with open("mapping.json", "r") as mapping_file:
        mapping = json.load(mapping_file)
    params = load_json("parameters.json")

    coeffs = pd.read_csv("ci.csv", index_col=0)
    coeffs["molecule_name"] = mapping[:-8]
    coeffs = coeffs.set_index("molecule_name")
    fig, axes = plot_intensities(coeffs, params["lag"])

    file_name = "ci-gender={}-bucket-size={}-lag={}.pdf".format(
        params["gender"], params["bucket"], params["lag"]
    )
    with PdfPages(file_name) as pdf:
        pdf.savefig(fig)
