"""If the cross validation results have already been dumped, read best cv param
and run a fit with parallel bootstrap using these parameters."""

import json
import pickle

import numpy as np
from tick.survival.sccs import StreamConvSCCS


def read_parameters() -> dict:
    with open("parameters.json", "r") as parameters_file:
        parameters_json = "".join(parameters_file.readlines())
        return json.loads(parameters_json)


def unpickle(filepath):
    with open(filepath, "rb") as file:
        return pickle.load(file)


def write_json(obj, filepath):
    with open(filepath, "w") as file:
        json.dump(obj, file)


def read_json(filepath):
    with open(filepath, "r") as file:
        data = json.load(file)
    return data


if __name__ == "__main__":
    print("Read inputs")
    features = unpickle("features")
    labels = unpickle("labels")
    censoring = unpickle("censoring")
    mapping = unpickle("mapping")
    n_age_groups = unpickle("age_groups")
    cv_track = read_json("cv_track.json")
    c_tv = cv_track['best_model']['C_tv']
    c_group_l1 = cv_track['best_model']['C_group_l1']

    print("Read parameters")
    n_mols = len(mapping) - n_age_groups

    parameters = read_parameters()
    mols_lags = parameters["lag"]

    n_lags = np.repeat(mols_lags, n_mols)
    penalized_features = np.arange(n_mols)

    features_wo_age = [f[:, :n_mols] for f in features]

    X = features_wo_age
    y = [l.ravel() for l in labels]
    c = censoring.ravel()

    model = StreamConvSCCS(
        n_lags,
        penalized_features,
        max_iter=500,
        verbose=True,
        record_every=1,
        print_every=10,
        tol=1e-5,
        threads=15,
        C_tv=c_tv,
        C_group_l1=c_group_l1,
    )

    estimates, bootstrap = model.fit(X, y, c,
                                     confidence_intervals=True,
                                     n_samples_bootstrap=100)

    write_json(bootstrap, "bootstrap.json")


