import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
from matplotlib import cm
import json
from matplotlib.backends.backend_pdf import PdfPages

plt.style.use(["ggplot"])


def plot_learning_curves_contour(self, best_params, elevation=25, azimuth=35):
    """‘elev’ stores the elevation angle in the z plane.
        ‘azim’ stores the azimuth angle in the x,y plane."""
    sc_train = self.kfold_mean_train_scores.values
    sc_test = self.kfold_mean_test_scores.values
    X_tile = np.log10(self.C_tv_history.values)
    Y_tile = np.log10(self.C_group_l1_history.values)

    fig, axarr = plt.subplots(1, 3, figsize=(12, 4), sharey=True, sharex=True)

    ax = axarr[-1]
    ax.scatter(X_tile, Y_tile)
    ax.set_title("Random search tested hyperparameters")
    ax.set_xlabel("Strength TV")
    ax.set_ylabel("Strength Group L1")

    names = ["train", "test"]
    cmaps = [cm.RdBu, cm.RdBu]

    for i, cv in enumerate([sc_train, sc_test]):
        Z = np.array(cv)
        ax = axarr[i]
        cax = ax.tricontourf(X_tile, Y_tile, Z, 100, cmap=cmaps[i])
        ax.set_title(r"Loss (%s)" % names[i])
        ax.set_xlabel("TV level (log)")
        idx = np.where(Z == Z.min())
        x, y = (X_tile[idx][0], Y_tile[idx][0])
        ax.scatter(x, y, color="red", marker="x")
        ax.text(x, y, r"%.2f" % Z.min(), color="red", fontsize=12)

    axarr[0].set_ylabel("Group L1 level (log)")
    plt.tight_layout()

    fig2 = plt.figure(figsize=(8, 6.5))
    ax = Axes3D(fig2)
    colors = ["blue", "green"]
    names = ["train", "test"]
    proxies = []
    proxy_names = []
    for i, cv in enumerate([sc_train, sc_test]):
        Z = np.array(cv)
        ax.plot_trisurf(X_tile, Y_tile, Z, alpha=0.3, color=colors[i])
        proxies.append(plt.Rectangle((0, 0), 1, 1, fc=colors[i], alpha=0.3))
        proxy_names.append("%s score" % names[i])

    Z = np.array(sc_test)
    x = np.log10(best_params["C_tv"])
    y = np.log10(best_params["C_group_l1"])
    idx = np.where(X_tile == x)  # should be equal to np.where(Y_tile == y)
    z = Z[idx]
    p1 = ax.scatter(x, y, z, c="red")
    proxies.append(p1)
    proxy_names.append("CV best score")

    ax.set_xlabel("TV level (log)")
    ax.set_ylabel("Group L1 level (log)")
    ax.set_title("Learning surfaces")
    ax.set_zlabel("loss")
    ax.view_init(elevation, azimuth)
    plt.legend(proxies, proxy_names, loc="best")
    return fig, axarr, fig2, ax


if __name__ == "__main__":
    with open("cv_track.json", "r") as file:
        cv_track = json.load(file)
    results = pd.DataFrame(
        {
            k: v
            for k, v in cv_track.items()
            if len(cv_track["kfold_mean_train_scores"]) == len(v)
        }
    )
    fig, axarr, fig2, ax = plot_learning_curves_contour(results, cv_track["best_model"])
    with PdfPages("cv_results.pdf") as pdf:
        pdf.savefig(fig)
        pdf.savefig(fig2)
