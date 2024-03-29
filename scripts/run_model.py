import json
import pickle

import numpy as np
import pandas as pd
from tick.survival.sccs import StreamConvSCCS


class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)


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


# TODO: refactor this
def get_lag(event_value):
    lag = 29 # default value
    if event_value[:4] == "pre-":
        lag = 13
    elif event_value[0] == '[':
        lag = None
    return lag


if __name__ == "__main__":
    print("Read inputs")
    features = unpickle("features")
    labels = unpickle("labels")
    censoring = unpickle("censoring")
    mapping = unpickle("mapping")
    n_age_groups = unpickle("age_groups")

    print("Read parameters")
    n_mols = len(mapping) - n_age_groups

    parameters = read_parameters()
    mols_lags = parameters["lag"] - 15  # TODO: delay parameter
    pre_exposures_lags = 14 - 1  # TODO: delay parameter

    n_lags = np.array([get_lag(val) for val in mapping if get_lag(val) is not None])

    # n_lags = np.hstack([np.repeat(mols_lags, n_mols),
    #                     np.repeat(pre_exposures_lags, n_mols)])

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
    )

    print(
        "Number of Samples {}; Number of Features {}; Number of buckets {}".format(
            len(X), X[0].shape[1], X[0].shape[0]
        )
    )

    coeffs, cv_track = model.fit_kfold_cv(
        X, y, c, C_tv_range=(2, 5), C_group_l1_range=(3, 9), n_cv_iter=100, n_folds=3
    )

    print("Saving results")
    pd.DataFrame(coeffs).to_csv("coeffs.csv")
    print("Saving CV tracker")
    write_json(cv_track.todict(), "cv_track.json")

    best_parameters = cv_track.find_best_params()

    model_custom = StreamConvSCCS(
        n_lags,
        penalized_features,
        max_iter=500,
        C_tv=best_parameters["C_tv"],
        C_group_l1=best_parameters["C_group_l1"],
        verbose=True,
        record_every=1,
        print_every=10,
        tol=1e-5,
        threads=10,
        step=model.step
    )
    print("Launching Bootstrap")
    coeffs_custom, ci = model_custom.fit(
        X, y, c, confidence_intervals=True, n_samples_bootstrap=100
    )
    print("Saving bootstrap results")
    pd.DataFrame(coeffs_custom).to_csv("coefficients_custom.csv")
    # pd.DataFrame(ci).to_csv("ci.csv")
    with open("ci.json", "w") as file:
        json.dump(ci, file, cls=NumpyEncoder)

    print("Dumping the Mapping to Json formats")
    write_json(mapping, "mapping.json")
