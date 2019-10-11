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
STUDY_START = pytz.datetime.datetime(2014, 1, 1, tzinfo=pytz.UTC)
STUDY_END = pytz.datetime.datetime(2017, 1, 1, 23, 59, 59, tzinfo=pytz.UTC)
AGE_REFERENCE_DATE = pytz.datetime.datetime(2017, 1, 1, tzinfo=pytz.UTC)


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


@register
def gender_distribution(logger: Logger, cohort: Cohort, step: str):
    data = agg(cohort.subjects, frozenset(["gender"]), "count")
    data.gender = data.gender.replace({1: "Male", 2: "Female"})
    data.columns = ["gender", "n_patients"]
    # Sad, but required with old pandas
    gender_distribution = json.loads(data.to_json(orient="records"))
    return logger(step, "gender_distribution", gender_distribution)


@register
def distribution_by_age_bucket(logger: Logger, cohort: Cohort, step: str):
    data = agg(cohort.subjects, frozenset(["ageBucket"]), "count").sort_values(
        "ageBucket"
    )
    data.ageBucket = data.ageBucket.map(lambda x: BUCKET_INTEGER_TO_STR[x])
    data.columns = ["ageBucket", "n_patients"]
    # Sad, but required with old pandas
    age_distribution = json.loads(data.to_json(orient="records"))
    return logger(step, "age_distribution", age_distribution)


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
    data.columns = ["n_weeks", "n_patients"]
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
    group_columns = ["patientID", "value"]
    data = agg(cohort.events, frozenset(group_columns), "count")
    data = _get_distinct(data, group_columns)
    data = data.groupby("patientID").count().reset_index().groupby("value").count()
    data.columns = ["n_patients"]
    data["n_events"] = data.index
    data.astype("int")
    unique_events_distribution_per_patient = json.loads(
        data.to_json(orient="records")
    )  # Sad, but required with old pandas
    return logger(
        step,
        "unique_events_distribution_per_patient",
        unique_events_distribution_per_patient,
    )


@register
def log_patient_distribution_per_unique_event(
    logger: Logger, cohort: Cohort, step: str
):
    """Number of distinct patients count per event"""
    group_columns = ["patientID", "value"]
    data = _get_distinct(cohort.events, group_columns)
    data = agg(data[group_columns], frozenset(["value"]), "count")
    data.columns = ["n_patients", "event_name"]
    patient_distribution_per_event = json.loads(
        data.to_json(orient="records")
    )  # Sad, but required with old pandas
    return logger(
        step, "patient_distribution_per_event", patient_distribution_per_event
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
    logger = get_logger()
    buffer = md.get("fractures").events
    buffer = buffer.withColumnRenamed("value", "temp")
    buffer = buffer.withColumnRenamed("groupID", "value")
    buffer = buffer.withColumnRenamed("temp", "groupID")
    md.get("fractures").events = buffer

    flow_json = """
    {
        "intermediate_operations": {
        },
        "steps": [
            "extract_patients",
            "exposures",
            "filter_patients",
            "fractures"
        ]
    }
    """
    logger.info("Flowchart preparation.")
    flow, md = experience_to_flowchart_metadata(md, read_parameters())
    md.add_subjects_information("omit_all", AGE_REFERENCE_DATE)
    exposure_steps = flow.create_flowchart(md.get("exposures"))
    fracture_steps = flow.create_flowchart(md.get("fractures"))

    plot_functions = [
        distribution_by_gender_age_bucket,
        plot_patient_distribution_per_unique_event,
        plot_unique_event_distribution_per_patient,
    ]

    logger.info("Saving stats to PDF files.")
    # Save plots in pdf
    save_plots(plot_functions, exposure_plots, exposure_steps, figsize=(16, 9))
    save_plots(plot_functions, fracture_plots, fracture_steps, figsize=(16, 9))

    # Log stats
    logger.info("Logging stats to json files.")
    log_steps = json.loads(flow_json)["steps"]
    log = Logger(log_steps, prefix="exposures")

    for step_name, cohort in zip(log_steps, exposure_steps):
        for log_func in registry:
            log_func(log, cohort, step_name)

    log.save(exposure_logs)

    log_steps = json.loads(flow_json)["steps"]
    log = Logger(log_steps, prefix="fractures")

    for step_name, cohort in zip(log_steps, exposure_steps):
        for log_func in registry:
            log_func(log, cohort, step_name)

    log.save(fracture_logs)
