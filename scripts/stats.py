import matplotlib
import json

import pyspark.sql.functions as sf
import pytz

from scalpel.core.cohort import Cohort
from scalpel.core.io import get_logger, quiet_spark_logger, get_sql_context
from scalpel.core.cohort_collection import CohortCollection
from scalpel.core.util import rename_df_columns
import pandas as pd
from scalpel.stats.graph_utils import BUCKET_INTEGER_TO_STR
from scalpel.stats.grouper import agg

from parameters.experience import (
    read_cohort_collection,
    read_parameters,
    experience_to_flowchart_metadata,
)
from parameters.fall_parameters import STUDY_END, STUDY_START, AGE_REFERENCE_DATE

matplotlib.use("Agg")


def delete_invalid_events(
    cohort: Cohort, study_start: pytz.datetime, study_end: pytz.datetime
):
    valid_start = sf.col("start").between(study_start, study_end)
    valid_end = sf.col("end").between(study_start, study_end)
    valid_event = valid_start & valid_end
    invalid_events = cohort.events.where(~valid_event)
    blacklist = Cohort(
        "", "", invalid_events.select("patientID").drop_duplicates(), None
    )
    return cohort.difference(blacklist)


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


def clean_metadata(
    cc: CohortCollection, study_start: pytz.datetime, study_end: pytz.datetime
) -> CohortCollection:
    clean_cc = {}
    cohorts = cc.cohorts_names()
    for k in cohorts:
        v = cc.get(k)
        if v.events is not None:
            clean_cohort = delete_invalid_events(v, study_start, study_end)
            if k == "fractures":
                followups = cc.get("follow_up")
                clean_cohort = delete_prevalent(clean_cohort, followups)
        else:
            clean_cohort = v
        clean_cc[k] = clean_cohort
    return CohortCollection(clean_cc)


class Logger:
    def __init__(self, steps, prefix=None):
        self.log = {step_name: dict() for step_name in steps}
        self.log["prefix"] = prefix
        self.prefix = prefix

    def __call__(self, step, logname, value):
        if self.log[step].get(logname) is None:
            self.log[step][logname] = value
        else:
            raise ValueError("{} is not None at step {}".format(logname, step))
        return self

    def __str__(self):
        if self.prefix is not None:
            log = {
                self.prefix + "_at_step_" + step_name: self.log[step_name]
                for step_name in self.log.keys()
            }
            log["prefix"] = self.log["prefix"]
        else:
            log = self.log.copy()
        return str(log)

    def save(self, filename):
        with open(filename, "w") as f:
            json.dump(self.log, f)

    def load(self, filename):
        with open(filename, "r") as f:
            self.log = json.load(f)
            self.prefix = self.log.get("prefix")


def _get_distinct(data, group_columns) -> pd.DataFrame:
    return data[group_columns].drop_duplicates()


registry = []


def register(f):
    registry.append(f)
    return f


@register
def log_number_patients(logger: Logger, cohort: Cohort, step: str):
    n_patients = cohort.subjects.count()
    return logger(step, "n_patients", n_patients)


# @register
def gender_distribution(logger: Logger, cohort: Cohort, step: str):
    data = agg(cohort.subjects, frozenset(["gender"]), "count")
    data.gender = data.gender.replace({1: "Male", 2: "Female"})
    data.rename(columns={"count(1)": "n_patients"}, inplace=True)
    # Sad, but required with old pandas
    gender_distribution = json.loads(data.to_json(orient="records"))
    return logger(step, "gender_distribution", gender_distribution)


# @register
def distribution_by_age_bucket(logger: Logger, cohort: Cohort, step: str):
    data = agg(cohort.subjects, frozenset(["ageBucket"]), "count").sort_values(
        "ageBucket"
    )
    data.ageBucket = data.ageBucket.map(lambda x: BUCKET_INTEGER_TO_STR[x])
    data.rename(columns={"count(1)": "n_patients"}, inplace=True)
    # Sad, but required with old pandas
    age_distribution = json.loads(data.to_json(orient="records"))
    return logger(step, "age_distribution", age_distribution)


@register
def distribution_by_age_bucket(logger: Logger, cohort: Cohort, step: str):
    data = agg(
        cohort.subjects, frozenset(["gender", "ageBucket"]), "count"
    ).sort_values(["gender", "ageBucket"])
    data.gender = data.gender.replace({1: "Male", 2: "Female"})
    data.ageBucket = data.ageBucket.map(lambda x: BUCKET_INTEGER_TO_STR[x])
    data.rename(columns={"count(1)": "n_patients"}, inplace=True)
    # Sad, but required with old pandas
    gender_age_distribution = json.loads(data.to_json(orient="records"))
    return logger(step, "gender_age_distribution", gender_age_distribution)


@register
def log_number_censored_patients(logger: Logger, cohort: Cohort, step: str):
    n_censored_patients = cohort.subjects.where(~sf.isnull("deathDate")).count()
    return logger(step, "n_censored_patients", n_censored_patients)


@register
def log_n_weeks_between_censoring_date_and_first_event_distribution(
    logger: Logger, cohort: Cohort, step: str
):
    censored_patients = cohort.subjects.where(~sf.isnull("deathDate"))
    data = (
        cohort.events.join(censored_patients, on="patientID", how="inner")
        .select("patientID", "start", "deathDate")
        .groupby("patientID", "deathDate")
        .agg(sf.min("start").alias("start"))
        .select(
            "patientID",
            sf.floor(sf.datediff("deathDate", "start") / 7).alias("n_weeks"),
        )
        .groupby("n_weeks")
        .count()
        .toPandas()
    )
    data.rename(columns={"count(1)": "n_patients"}, inplace=True)
    # Sad, but required with old pandas
    n_weeks_between_censoring_date_and_first_event_distribution = json.loads(
        data.to_json(orient="records")
    )
    return logger(
        step,
        "n_weeks_between_censoring_date_and_first_event_distribution",
        n_weeks_between_censoring_date_and_first_event_distribution,
    )


@register
def log_unique_event_distribution_per_patient(
    logger: Logger, cohort: Cohort, step: str
):
    """Number of distinct events per subject"""
    data = agg(
        cohort.events.select("patientID", "value").drop_duplicates(),
        frozenset(["patientID"]),
        "count",
    )
    data.rename(columns={"count(1)": "n_events"}, inplace=True)
    data = data.groupby("n_events").count().reset_index()
    data.rename(columns={"patientID": "n_patients"}, inplace=True)
    unique_events_distribution_per_patient = json.loads(
        data.to_json(orient="records")
    )  # Sad, but required with old pandas
    return logger(
        step,
        "unique_events_distribution_per_patient",
        unique_events_distribution_per_patient,
    )


def log_duration_distribution_per_event_type(logger: Logger, cohort: Cohort, step: str):
    """Count number of events per duration for each event type."""
    assert cohort.is_duration_events()
    data = agg(cohort.events, frozenset(["value", "duration"]), "count")
    data.rename(columns={"count(1)": "n_patients", "value": "event_name"}, inplace=True)
    duration_distribution_per_event_type = json.loads(data.to_json(orient="records"))
    return logger(
        step,
        "duration_distribution_per_event_type",
        duration_distribution_per_event_type,
    )


@register
def log_demographics_per_event_type(logger: Logger, cohort: Cohort, step: str):
    """Age and gender distribution of subjects associated with each event type."""
    associated_subjects = cohort.events.select("patientID", "value").drop_duplicates()
    associated_subjects = associated_subjects.join(
        cohort.subjects, on="patientID", how="left"
    )
    data = agg(
        associated_subjects, frozenset(["value", "gender", "ageBucket"]), "count"
    )
    data.gender = data.gender.replace({1: "Male", 2: "Female"})
    data.ageBucket = data.ageBucket.map(lambda x: BUCKET_INTEGER_TO_STR[x])
    data.rename(columns={"count(1)": "n_patients"}, inplace=True)
    demographics_per_event_type = json.loads(data.to_json(orient="records"))
    return logger(step, "demographics_per_event_type", demographics_per_event_type)


def cache_cohort(cohort: Cohort) -> Cohort:
    cohort.subjects.cache()
    if cohort.events is not None:
        cohort.events.cache()
    return cohort


def cache_metadata(metadata: CohortCollection) -> CohortCollection:
    for cohort in metadata:
        cache_cohort(metadata.get(cohort))
    return metadata


# PARAMETERS

metadata_path = "metadata_fall.json"
exposure_plots = "exposure_plots.pdf"
fracture_plots = "fracture_plots.pdf"
exposure_logs = "exposure_logs.json"
fracture_logs = "fracture_logs.json"

# END PARAMETERS

if __name__ == "__main__":
    sqlContext = get_sql_context()
    quiet_spark_logger(sqlContext.sparkSession)
    sqlContext.sparkSession.conf.set("spark.sql.session.timeZone", "UTC")

    old_md = clean_metadata(
        read_cohort_collection(metadata_path), STUDY_START, STUDY_END
    )

    logger = get_logger()
    buffer = old_md.get("fractures").events
    buffer = buffer.withColumnRenamed("value", "temp")
    buffer = buffer.withColumnRenamed("groupID", "value")
    buffer = buffer.withColumnRenamed("temp", "groupID")
    old_md.get("fractures").events = buffer
    cache_metadata(old_md)

    logger.info("Flowchart preparation.")
    flow, md = experience_to_flowchart_metadata(old_md, read_parameters())
    md.add_subjects_information("omit_all", AGE_REFERENCE_DATE)
    cache_metadata(md)

    logger.info("Preparing exposures.")
    exposures = md.get("exposures")
    base_exposure = Cohort(
        "all_exposed_patients", "", exposures.subjects, exposures.events
    )
    base_exposure.add_duration_information()

    logger.info("Preparing fractures.")
    fractures = md.get("fractures")
    base_fracture = Cohort(
        "all_fracture cases", "", fractures.subjects, fractures.events
    )
    logger.info("Fractures Flowchart")
    fracture_steps = flow.prepend_cohort(base_fracture)
    logger.info("Exposure Flowchart")
    exposure_steps = flow.prepend_cohort(base_exposure)

    # Log stats
    logger.info("Logging stats to json files.")

    # Stats on exposures
    log_steps = [c.name for c in exposure_steps.ordered_cohorts]
    log = Logger(log_steps, prefix="exposures")
    for step_name, cohort in zip(log_steps, exposure_steps.steps):
        cache_cohort(cohort)
        get_logger().info("Step name {} for exposures".format(step_name))
        for log_func in registry:
            get_logger().info("Logging {}".format(log_func.__name__))
            log_func(log, cohort, step_name)
        log_duration_distribution_per_event_type(log, cohort, step_name)
    log.save(exposure_logs)

    # Stats on fractures
    log_steps = [c.name for c in fracture_steps.ordered_cohorts]
    log = Logger(log_steps, prefix="fractures")
    for step_name, cohort in zip(log_steps, fracture_steps.steps):
        cache_cohort(cohort)
        get_logger().info("Step name {} for fractures".format(step_name))
        for log_func in registry:
            get_logger().info("Logging {}".format(log_func.__name__))
            log_func(log, cohort, step_name)
    log.save(fracture_logs)
