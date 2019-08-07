import matplotlib

matplotlib.use("Agg")
import abc
import json
import pickle

import pyspark.sql.functions as sf
import pytz
from pyspark.sql import Window
from src.exploration.core.cohort import Cohort
from src.exploration.core.io import get_logger, get_spark_context, quiet_spark_logger
from src.exploration.core.metadata import Metadata
from src.exploration.core.util import rename_df_columns
import pandas as pd
from src.exploration.core.decorators import save_plots
from src.exploration.core.flowchart import Flowchart
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


class Parameter(abc.ABC):
    def __init__(self, value):
        self.value = value

    @abc.abstractmethod
    def log(self) -> str:
        """
        Logs the current parameter filtering
        :return: str
        """

    @abc.abstractmethod
    def filter(self, cohort: Cohort) -> Cohort:
        """Applies the parameter based on the passed value"""


class GenderParameter(Parameter):
    def log(self):
        return "Applying gender filter."

    def filter(self, cohort: Cohort) -> Cohort:
        if self.value == "homme":
            return Cohort(
                cohort.name,
                cohort.characteristics,
                cohort.subjects.where(sf.col("gender") == 1),
                None,
            )
        elif self.value == "femme":
            return Cohort(
                cohort.name,
                cohort.characteristics,
                cohort.subjects.where(sf.col("gender") == 2),
                None,
            )
        elif self.value == "all":
            return cohort
        else:
            raise ValueError(
                "Gender must be homme, femme or all. You entered {}".format(self.value)
            )


class OldSubjectsParameter(Parameter):
    def log(self) -> str:
        return "Applying old subjects filter."

    def filter(self, cohort: Cohort) -> Cohort:
        if self.value is True:
            return cohort
        else:
            young_subjects = cohort.subjects.where(sf.col("age") < 85)
            return Cohort("Young subjects", "Young subjects", young_subjects, None)


class FractureSiteParameter(Parameter):
    def log(self) -> str:
        return "Applying site fracture filter."

    def filter(self, cohort: Cohort) -> Cohort:
        if self.value == "all":
            return cohort
        else:
            events = cohort.events.where(sf.col("groupID") == self.value)
            if events.count() == 0:
                raise ValueError(
                    "Le site {} n'existe pas dans la cohorte de fractures".format(
                        self.value
                    )
                )
            else:
                return Cohort(
                    "Fractures on {}".format(self.value),
                    "Subjects with fractures on site {}".format(self.value),
                    events.select("patientID").distinct(),
                    events,
                )


class MultiFracturedParameter(Parameter):
    def log(self) -> str:
        return "Applying Multi-fractured filter"

    def filter(self, cohort: Cohort) -> Cohort:
        if self.value == True:
            return cohort
        else:
            fractured_once_per_admission = (
                cohort.events.groupBy(["patientID", "start"])
                .count()
                .where(sf.col("count") == 1)
                .select("patientID")
                .distinct()
            )
            new_fractures = cohort.events.join(
                fractured_once_per_admission, "patientID"
            )
            return Cohort(
                "Fractures",
                "{} that has only one fracture per admission".format(
                    cohort.characteristics
                ),
                new_fractures.select("patientID").distinct(),
                new_fractures,
            )


class MultiAdmissionParameter(Parameter):
    def log(self) -> str:
        return "Applying Multi-admission filter"

    def filter(self, cohort: Cohort) -> Cohort:
        if self.value == True:
            return cohort
        else:
            admitted_once = (
                cohort.events.groupBy(["patientID", "start"])
                .count()
                .groupby("patientID")
                .count()
                .where(sf.col("count") == 1)
            )
            new_fractures = cohort.events.join(admitted_once, "patientID")
            return Cohort(
                "Fractures",
                "{} that has only one admission for fracture".format(
                    cohort.characteristics
                ),
                new_fractures.select("patientID").distinct(),
                new_fractures,
            )


def read_metadata(file_path: str) -> Metadata:
    with open(file_path, "r") as metadata_file:
        metadata_txt = "".join(metadata_file.readlines())
        return Metadata.from_json(metadata_txt)


def pickle_object(obj, path):
    with open(path, "wb") as file:
        pickle.dump(obj, file)


def keep_elderly_filter(subjects: Cohort, keep_elderly: bool) -> Cohort:
    if keep_elderly is True:
        return subjects
    else:
        young_subjects = subjects.subjects.where(sf.col("age") < 85)
        return Cohort("Young subjects", "Young subjects", young_subjects, None)


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


def site_filter(outcomes: Cohort, site: str) -> Cohort:
    if site == "all":
        return outcomes
    else:
        events = outcomes.events.where(sf.col("groupID") == site)
        if events.count() == 0:
            raise ValueError(
                "Le site {} n'existe pas dans la cohorte de fractures".format(site)
            )
        else:
            return Cohort(
                "Fractures on {}".format(site),
                "Subjects with fractures on site {}".format(site),
                events.select("patientID").distinct(),
                events,
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


def prepare_metadata() -> Metadata:
    logger = get_logger()

    valid_start = sf.col("start").between(STUDY_START, STUDY_END)

    logger.info("Reading parameters")
    parameters = read_parameters()
    json_file_path = parameters["path"]
    gender = parameters["gender"]
    site = parameters["site"]
    keep_elderly = parameters["keep_elderly"]
    keep_multi_fractured = parameters["keep_multi_fractured"]
    keep_multi_admitted = parameters["keep_multi_admitted"]

    gender_param = GenderParameter(gender)
    keep_elderly_param = OldSubjectsParameter(keep_elderly)

    subjects_parameters = [gender_param, keep_elderly_param]

    site_param = FractureSiteParameter(site)
    multi_fractured_param = MultiFracturedParameter(keep_multi_fractured)
    multi_admitted_param = MultiAdmissionParameter(keep_multi_admitted)

    fracture_parameters = [site_param, multi_fractured_param, multi_admitted_param]

    logger.info("Reading metadata")
    md = read_metadata(json_file_path)

    base_cohort = md.get("extract_patients")
    cache_cohort(base_cohort)

    base_cohort.add_age_information(AGE_REFERENCE_DATE)
    for param in subjects_parameters:
        logger.info(param.log())
        base_cohort = param.filter(base_cohort)

    md.add_cohort("extract_patients", base_cohort)

    outcomes_clean = md.get("fractures").events.where(valid_start).cache()

    outcomes = Cohort(
        "Clean-fractures",
        "Subjects with fractures within study start and end",
        outcomes_clean.select("patientID").distinct(),
        outcomes_clean,
    )

    for param in fracture_parameters:
        logger.info(param.log())
        outcomes = param.filter(outcomes)

    md.add_cohort("fractures", outcomes)
    cache_metadata(md)

    md.get("filter_patients").add_age_information(AGE_REFERENCE_DATE)


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

    md = prepare_metadata()
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
    flow = Flowchart.from_json(md, flow_json)
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
