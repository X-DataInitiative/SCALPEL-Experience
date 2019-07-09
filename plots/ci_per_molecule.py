import pandas as pd
import json
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import numpy as np
import os


def load_json(filepath):
    with open(filepath) as file:
        return json.load(file)


def parse(string):
    return [float(value) for value in string[1:-1].split(" ") if value != ""]


def plot_ci(ax, intensity, upper_bound, lower_bound, mols_lags, gender):
    x = np.arange(mols_lags + 1)
    plot_intensity(ax, intensity, mols_lags, gender)
    ax.fill_between(x, np.exp(lower_bound), np.exp(upper_bound), alpha=0.5, step="post")
    return ax


def plot_intensity(ax, intensity, mols_lags, gender):
    x = np.arange(mols_lags + 1)
    if mols_lags % 5 == 0:
        ax.xaxis.set_major_locator(plt.IndexLocator(5, 0))
    elif mols_lags % 6 == 0:
        ax.xaxis.set_major_locator(plt.IndexLocator(3, 0))
    ax.xaxis.set_minor_locator(plt.IndexLocator(1, 0))
    ax.grid(True, which="minor", axis="x", linestyle="--")

    ax.step(x, np.exp(intensity), where="post")
    ax.hlines(1, 0, max(x))
    ax.set_title("lag={}".format(mols_lags))
    ax.set_ylabel("gender={}".format(gender))
    return ax


def read_coeffs_data(dir_path, file) -> pd.DataFrame:
    try:
        coeffs = pd.read_csv(os.path.join(dir_path, file), index_col=0)
    except FileNotFoundError:
        coeffs = pd.DataFrame()

    params = load_json(os.path.join(dir_path, "parameters.json"))
    mapping = load_json(os.path.join(dir_path, "mapping.json"))

    coeffs["molecule_name"] = mapping[:-8]
    coeffs["gender"] = params["gender"]
    coeffs["lag"] = params["lag"]
    coeffs["bucket_size"] = params["bucket"]
    lag = "lag={0:0>2}".format(params["lag"])
    return (
        "bucket-size={}-gender={}-{}".format(params["bucket"], params["gender"], lag),
        coeffs,
    )


def read_result_data_frame(name):
    df_s = dict()
    for dir_path, dirs, _ in os.walk("./"):
        try:
            k, v = read_coeffs_data(dir_path, name)
            df_s[k] = v
        except FileNotFoundError:
            pass

    return df_s


def plot_molecule_ci(molecule, df_s, n_cols=3):
    fig, axes = plt.subplots(6, n_cols, figsize=(12, 24), sharey=True, sharex=False)
    for (i, (k, df)) in enumerate(sorted(df_s.items())):
        try:
            intensity = parse(df[df.molecule_name == molecule].refit_coeffs.values[0])
            upper = parse(df[df.molecule_name == molecule].lower_bound.values[0])
            lower = parse(df[df.molecule_name == molecule].upper_bound.values[0])
            plot_ci(
                axes[i // n_cols][i % n_cols],
                intensity,
                upper,
                lower,
                df[df.molecule_name == molecule].lag.values[0],
                df[df.molecule_name == molecule].gender.values[0],
            )
        except IndexError:
            pass
        except AttributeError:
            pass
    plt.tight_layout()

    return fig


def make_ci():
    name = "ci.csv"
    df_s = read_result_data_frame(name)
    n_cols = 3
    plt.style.use(["seaborn-ticks", "ggplot"])
    dir_name = "ci_per_molecule"
    try:
        os.mkdir(dir_name)
    except FileExistsError:
        pass
    mapping = load_json("mapping.json")

    for molecule in mapping:
        file_name = "{}.pdf".format(molecule)
        with PdfPages(os.path.join(dir_name, file_name)) as pdf:
            pdf.savefig(plot_molecule_ci(molecule, df_s, n_cols))


if __name__ == "__main__":
    make_ci()
