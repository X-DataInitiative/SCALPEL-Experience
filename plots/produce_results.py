import json

import matplotlib.pyplot as plt
from math import ceil
import numpy as np
import pandas as pd
from matplotlib.backends.backend_pdf import PdfPages

plt.style.use(["ggplot"])


def plot_intensity(ax, molecule_name, intensity, mols_lags):
    x = np.arange(mols_lags + 1)
    ax.step(x, np.exp(intensity), where="post")
    ax.hlines(1, 0, max(x))
    ax.set_title(molecule_name)
    ax.set_ylim(0.8, 2.5)
    return ax


def plot_intensities(df: pd.DataFrame, mols_lags, n_cols=3):
    fig, axes = plt.subplots(
        ceil(len(df) / n_cols), n_cols, sharey=False, figsize=(16, 82)
    )
    for (i, row) in enumerate(df.iterrows()):
        plot_intensity(axes[i // n_cols][i % n_cols], row[0], row[1].values, mols_lags)
    plt.tight_layout()
    return fig, axes


def prepare_coeffs_dataframe():
    coeffs = pd.read_csv("coeffs.csv", index_col=0)
    with open("mapping.json", "r") as mapping_file:
        mapping = json.load(mapping_file)

    coeffs["molecule_name"] = mapping[:-8]
    coeffs = coeffs.set_index("molecule_name")
    return coeffs


def get_params():
    with open("parameters.json", "r") as parameters_file:
        parameters_json = "".join(parameters_file.readlines())
        return json.loads(parameters_json)


if __name__ == "__main__":
    params = get_params()

    fig, axes = plot_intensities(prepare_coeffs_dataframe(), params["lag"], 3)
    file_name = "gender={}-bucket-size={}-lag={}.pdf".format(
        params["gender"], params["bucket"], params["lag"]
    )
    with PdfPages(file_name) as pdf:
        pdf.savefig(fig)
