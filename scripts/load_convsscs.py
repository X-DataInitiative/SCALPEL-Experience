import pickle

import numpy as np
import pyspark.sql.functions as sf
import pytz
from scalpel.core.cohort import Cohort
from scalpel.core.io import get_logger, get_spark_context, quiet_spark_logger
from scalpel.core.util import rename_df_columns
from scalpel.drivers.conv_sccs import ConvSccsFeatureDriver

from parameters.experience import (
    get_exposures,
    get_fractures,
    get_subjects,
    get_the_followup,
    read_cohort_collection,
    read_parameters,
)

BUCKET_ROUNDING = "ceil"
RUN_CHECKS = True
STUDY_START = pytz.datetime.datetime(2010, 1, 1, tzinfo=pytz.UTC)
STUDY_END = pytz.datetime.datetime(2015, 1, 1, 23, 59, 59, tzinfo=pytz.UTC)
AGE_REFERENCE_DATE = pytz.datetime.datetime(2017, 1, 1, tzinfo=pytz.UTC)
AGE_GROUPS = [0, 64, 67, 70, 73, 76, 79, 80, np.Inf]


def pickle_object(obj, path):
    with open(path, "wb") as file:
        pickle.dump(obj, file)


def delete_prevalent(outcomes: Cohort, followup: Cohort) -> Cohort:
    fup_events = rename_df_columns(followup.events, prefix="fup_", keys=("patientID",))
    out_events = outcomes.events.join(fup_events, on="patientID")

    is_valid = (sf.col("start") >= sf.col("fup_start")) & (
        sf.col("start") <= sf.col("fup_end")
    )
    # no condition on outcome end, as it is always null in this study

    prevalent_events = out_events.where(~is_valid)

    prevalent_cases = Cohort(
        "prevalent cases",
        "prevalent_cases",
        subjects=prevalent_events.select("patientID").distinct(),
        events=prevalent_events.select(*outcomes.events.columns),
    )
    return outcomes.difference(prevalent_cases)


def main():
    sqlContext = get_spark_context()
    quiet_spark_logger(sqlContext.sparkSession)
    sqlContext.sparkSession.conf.set("spark.sql.session.timeZone", "UTC")
    logger = get_logger()

    valid_start = sf.col("start").between(STUDY_START, STUDY_END)

    logger.info("Reading parameters")
    parameters = read_parameters()
    logger.info("Reading metadata")
    json_file_path = parameters["path"]
    md = read_cohort_collection(json_file_path)

    clean_outcomes = md.get("fractures").events.where(valid_start)
    md = md.add_cohort(
        "fractures",
        Cohort(
            "fractures",
            "Subjects with fractures",
            clean_outcomes.select("patientID").distinct(),
            clean_outcomes,
        ),
    )

    logger.info("Add age information to patients")
    md.add_subjects_information("omit_all", AGE_REFERENCE_DATE)

    base_cohort = get_subjects(md, parameters)
    outcomes = get_fractures(md, parameters)
    followup = get_the_followup(md, parameters)
    exposures = get_exposures(md, parameters)

    min_base = base_cohort.intersect_all([followup, exposures, outcomes])
    min_base.add_subject_information(base_cohort, "omit_all")

    min_exp = exposures.intersection(min_base)
    min_out = outcomes.intersection(min_base)
    min_fup = followup.intersection(min_base)

    logger.debug("Min base subject count {}".format(min_base.subjects.count()))

    min_incident_out = delete_prevalent(min_out, min_fup)
    loader = ConvSccsFeatureDriver(
        min_base,
        min_fup,
        min_exp,
        min_incident_out,
        parameters["bucket"],
        STUDY_START,
        STUDY_END,
        AGE_REFERENCE_DATE,
        AGE_GROUPS,
        BUCKET_ROUNDING,
        RUN_CHECKS,
        outcomes_split_column="category",
    )
    logger.info("Loading features")
    features, labels, censoring = loader.load()
    mapping = loader.mappings[0]
    n_age_groups = loader.n_age_groups

    print("Number of samples {}".format(len(censoring)))
    pickle_object(features, "features")
    pickle_object(labels, "labels")
    pickle_object(censoring, "censoring")
    pickle_object(mapping, "mapping")
    pickle_object(n_age_groups, "age_groups")


if __name__ == "__main__":
    main()
