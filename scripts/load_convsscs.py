from src.exploration.core.cohort import Cohort
from src.exploration.core.io import get_spark_context, get_logger, quiet_spark_logger
from src.exploration.core.metadata import Metadata
from src.exploration.core.util import rename_df_columns
from src.exploration.loaders import ConvSccsLoader
import json
import pytz
import pickle
from pyspark.sql import Window
import pyspark.sql.functions as sf
import numpy as np


BUCKET_ROUNDING = "ceil"
RUN_CHECKS = True
STUDY_START = pytz.datetime.datetime(2010, 1, 1, tzinfo=pytz.UTC)
STUDY_END = pytz.datetime.datetime(2015, 1, 1, 23, 59, 59, tzinfo=pytz.UTC)
AGE_REFERENCE_DATE = pytz.datetime.datetime(2017, 1, 1, tzinfo=pytz.UTC)
AGE_GROUPS = [0, 64, 67, 70, 73, 76, 79, 80, np.Inf]


def read_metadata(file_path: str) -> Metadata:
    with open(file_path, "r") as metadata_file:
        metadata_txt = "".join(metadata_file.readlines())
        return Metadata.from_json(metadata_txt)


def gender_filter(cohort: Cohort, gender: str) -> Cohort:
    if gender == "homme":
        return Cohort(
            cohort.name,
            cohort.characteristics,
            cohort.subjects.where(sf.col("gender") == 1),
            None,
        )
    elif gender == "femme":
        return Cohort(
            cohort.name,
            cohort.characteristics,
            cohort.subjects.where(sf.col("gender") == 2),
            None,
        )
    elif gender == "all":
        return cohort
    else:
        raise ValueError(
            "Gender must be homme, femme or all. You entered {}".format(gender)
        )


def read_parameters() -> dict:
    with open("parameters.json", "r") as parameters_file:
        parameters_json = "".join(parameters_file.readlines())
        return json.loads(parameters_json)


def clean_followup(follow_up: Cohort, valid_start, valid_stop) -> Cohort:
    clean = follow_up.events.where((valid_start & valid_stop))
    return Cohort("Clean_fup", "clean fup", clean.select("patientID").distinct(), clean)


def keep_first_outcome(outcomes: Cohort) -> Cohort:
    window = Window.partitionBy("patientID").orderBy("start")
    events = (
        outcomes.events.withColumn("rn", sf.row_number().over(window))
        .where(sf.col("rn") == 1)
        .drop("rn")
    )
    return Cohort(
        outcomes.name,
        outcomes.characteristics,
        events.select("patientID").distinct(),
        events,
    )


def delete_prevalent(outcomes: Cohort, followup: Cohort) -> Cohort:
    fup_events = rename_df_columns(
        followup.events, prefix="fup_", keys=("patientID",)
    )
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
        events=prevalent_events.select(*min_out.events.columns),
    )
    return min_out.difference(prevalent_cases)


if __name__ == "__main__":
    sqlContext = get_spark_context()
    sqlContext.sparkSession.conf.set("spark.sql.session.timeZone", "UTC")
    logger = get_logger()
    quiet_spark_logger(sqlContext.sparkSession)

    valid_start = sf.col("start").between(STUDY_START, STUDY_END)
    valid_stop = sf.col("end").between(STUDY_START, STUDY_END)

    logger.info("Reading parameters")
    parameters = read_parameters()
    json_file_path = parameters["path"]
    gender = parameters["gender"]
    bucket_size = parameters["bucket"]

    logger.info("Reading metadata")
    md = read_metadata(json_file_path)

    logger.info("Loading cohorts")
    base_cohort = gender_filter(md.get("filter_patients"), gender)
    followup = md.get("follow_up")
    exposures = md.get("exposures")
    outcomes = md.get("fractures")

    logger.info("Cleaning cohorts")
    min_base = base_cohort.intersect_all([followup, exposures, outcomes])
    min_base.add_subject_information(base_cohort, "omit_all")
    min_fup = followup.intersection(min_base)
    min_exp = exposures.intersection(min_base)
    min_out = outcomes.intersection(min_base)

    min_fup = clean_followup(followup.intersection(min_base), valid_start, valid_stop)
    min_incident_out = delete_prevalent(min_out, min_fup)
    first_outcome = keep_first_outcome(min_incident_out)
    logger.info("Checking Cohorts with first outcome")
    loader = ConvSccsLoader(
        min_base,
        min_fup,
        min_exp,
        first_outcome,
        bucket_size,
        STUDY_START,
        STUDY_END,
        AGE_REFERENCE_DATE,
        AGE_GROUPS,
        BUCKET_ROUNDING,
        RUN_CHECKS,
        outcomes_split_column="category"
    )
    logger.info("Loading features")
    features, labels, censoring = loader.load()

    mapping = loader.mappings[0]
    n_age_groups = loader.n_age_groups

    pickle.dump(features, open("features", "wb"))
    pickle.dump(labels, open("labels", "wb"))
    pickle.dump(censoring, open("censoring", "wb"))
    pickle.dump(mapping, open("mapping", "wb"))
    pickle.dump(n_age_groups, open("age_groups", "wb"))
