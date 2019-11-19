import matplotlib

from parameters.experience import (
    read_metadata,
    read_parameters,
    experience_to_flowchart_metadata,
)

matplotlib.use("Agg")
import json

import pyspark.sql.functions as sf
import pytz
from src.exploration.core.cohort import Cohort
from src.exploration.core.io import get_logger, get_spark_context, quiet_spark_logger
from src.exploration.core.metadata import Metadata
from src.exploration.core.util import rename_df_columns
import pandas as pd
from src.exploration.core.decorators import save_plots
from src.exploration.stats.event_patient_distribution import (
    plot_patient_distribution_per_unique_event,
    plot_unique_event_distribution_per_patient,
)
from src.exploration.stats.graph_utils import BUCKET_INTEGER_TO_STR
from src.exploration.stats.grouper import agg
from src.exploration.stats.patients import distribution_by_gender_age_bucket


BUCKET_ROUNDING = "ceil"
RUN_CHECKS = True
STUDY_START = pytz.datetime.datetime(2013, 12, 31, 23, 59, 59, tzinfo=pytz.UTC)
STUDY_END = pytz.datetime.datetime(2017, 1, 1, tzinfo=pytz.UTC)
AGE_REFERENCE_DATE = pytz.datetime.datetime(2015, 1, 1, tzinfo=pytz.UTC)


def delete_invalid_events(cohort: Cohort, study_start: pytz.datetime,
                          study_end:pytz.datetime):
    valid_start = sf.col('start').between(study_start, study_end)
    valid_end = sf.col('end').between(study_start, study_end)
    valid_event = valid_start & valid_end
    invalid_events = cohort.events.where(~valid_event)
    blacklist = Cohort("", "", invalid_events.select("patientID").drop_duplicates(),
                       None)
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


def clean_metadata(metadata: Metadata, study_start: pytz.datetime,
                   study_end: pytz.datetime) -> Metadata:
    clean_metadata = {}
    cohorts = metadata.cohorts_names()
    for k in cohorts:
        v = metadata.get(k)
        if v.events is not None:
            clean_cohort = delete_invalid_events(v, study_start, study_end)
            if k == "fractures":
                followups = metadata.get('follow_up')
                clean_cohort = delete_prevalent(clean_cohort, followups)
        else:
            clean_cohort = v
        clean_metadata[k] = clean_cohort
    return Metadata(clean_metadata)


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
    data.rename(columns={'count(1)': 'n_patients'}, inplace=True)
    # Sad, but required with old pandas
    gender_distribution = json.loads(data.to_json(orient="records"))
    return logger(step, "gender_distribution", gender_distribution)


# @register
def distribution_by_age_bucket(logger: Logger, cohort: Cohort, step: str):
    data = agg(cohort.subjects, frozenset(["ageBucket"]), "count").sort_values(
        "ageBucket"
    )
    data.ageBucket = data.ageBucket.map(lambda x: BUCKET_INTEGER_TO_STR[x])
    data.rename(columns={'count(1)': 'n_patients'}, inplace=True)
    # Sad, but required with old pandas
    age_distribution = json.loads(data.to_json(orient="records"))
    return logger(step, "age_distribution", age_distribution)


@register
def distribution_by_age_bucket(logger: Logger, cohort: Cohort, step: str):
    data = agg(cohort.subjects, frozenset(["gender", "ageBucket"]), "count").sort_values(
        ["gender", "ageBucket"]
    )
    data.gender = data.gender.replace({1: "Male", 2: "Female"})
    data.ageBucket = data.ageBucket.map(lambda x: BUCKET_INTEGER_TO_STR[x])
    data.rename(columns={'count(1)': 'n_patients'}, inplace=True)
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
    data.rename(columns={'count(1)': 'n_patients'}, inplace=True)
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
    data = agg(cohort.events.select('patientID', 'value').drop_duplicates(),
               frozenset(['patientID']), "count")
    data.rename(columns={'count(1)': 'n_events'}, inplace=True)
    data = data.groupby("n_events").count().reset_index()
    data.rename(columns={'patientID': 'n_patients'}, inplace=True)
    unique_events_distribution_per_patient = json.loads(
        data.to_json(orient="records")
    )  # Sad, but required with old pandas
    return logger(
        step,
        "unique_events_distribution_per_patient",
        unique_events_distribution_per_patient,
    )


def log_duration_distribution_per_event_type(
        logger: Logger, cohort: Cohort, step: str
):
    """Count number of events per duration for each event type."""
    assert cohort.is_duration_events()
    data = agg(cohort.events, frozenset(["value", "duration"]), "count")
    data.rename(columns={'count(1)': 'n_patients', 'value': 'event_name'}, inplace=True)
    duration_distribution_per_event_type = json.loads(data.to_json(orient="records"))
    return logger(
        step, "duration_distribution_per_event_type",
        duration_distribution_per_event_type
    )


@register
def log_demographics_per_event_type(
        logger: Logger, cohort: Cohort, step: str
):
    """Age and gender distribution of subjects associated with each event type."""
    associated_subjects = cohort.events.select('patientID', 'value').drop_duplicates()
    associated_subjects = associated_subjects.join(cohort.subjects, on='patientID',
                                                   how='left')
    data = agg(associated_subjects, frozenset(['value', 'gender', 'ageBucket']),
               "count")
    data.gender = data.gender.replace({1: "Male", 2: "Female"})
    data.ageBucket = data.ageBucket.map(lambda x: BUCKET_INTEGER_TO_STR[x])
    data.rename(columns={'count(1)': 'n_patients'}, inplace=True)
    demographics_per_event_type = json.loads(data.to_json(orient="records"))
    return logger(
        step, "demographics_per_event_type",
        demographics_per_event_type
    )


def cache_cohort(cohort: Cohort) -> Cohort:
    cohort.subjects.cache()
    if cohort.events is not None:
        cohort.events.cache()

    return Cohort


def cache_metadata(metadata: Metadata) -> Metadata:
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
    sqlContext = get_spark_context()
    quiet_spark_logger(sqlContext.sparkSession)
    sqlContext.sparkSession.conf.set("spark.sql.session.timeZone", "UTC")

    md = read_metadata(metadata_path)
    md = clean_metadata(md, STUDY_START, STUDY_END)
    
    logger = get_logger()
    buffer = md.get("fractures").events
    buffer = buffer.withColumnRenamed("value", "temp")
    buffer = buffer.withColumnRenamed("groupID", "value")
    buffer = buffer.withColumnRenamed("temp", "groupID")
    md.get("fractures").events = buffer

    logger.info("Flowchart preparation.")
    flow, md = experience_to_flowchart_metadata(md, read_parameters())
    md.add_subjects_information("omit_all", AGE_REFERENCE_DATE)
    exposures = md.get("exposures")
    fractures = md.get("fractures")
    exposures.add_duration_information()
    exposure_steps = flow.create_flowchart(exposures)
    fracture_steps = flow.create_flowchart(fractures)

    # Log stats
    logger.info("Logging stats to json files.")

    # Stats on exposures
    log_steps = [c.name for c in exposure_steps.steps]
    log = Logger(log_steps, prefix="exposures")
    for cohort in exposure_steps.steps:
        step_name = cohort.name
        for log_func in registry:
            log_func(log, cohort, step_name)
        log_duration_distribution_per_event_type(log, cohort, step_name)
    log.save(exposure_logs)

    # Stats on fractures
    log_steps = [c.name for c in fracture_steps.steps]
    log = Logger(log_steps, prefix="fractures")
    for cohort in fracture_steps.steps:
        step_name = cohort.name
        for log_func in registry:
            log_func(log, cohort, step_name)
    log.save(fracture_logs)
