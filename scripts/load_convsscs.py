import abc
import json
import pickle

import numpy as np
import pyspark.sql.functions as sf
import pytz
from pyspark.sql import Window
from src.exploration.core.cohort import Cohort
from src.exploration.core.io import get_logger, get_spark_context, quiet_spark_logger
from src.exploration.core.metadata import Metadata
from src.exploration.core.util import rename_df_columns
from src.exploration.loaders import ConvSccsLoader

BUCKET_ROUNDING = "ceil"
RUN_CHECKS = True
STUDY_START = pytz.datetime.datetime(2010, 1, 1, tzinfo=pytz.UTC)
STUDY_END = pytz.datetime.datetime(2015, 1, 1, 23, 59, 59, tzinfo=pytz.UTC)
AGE_REFERENCE_DATE = pytz.datetime.datetime(2017, 1, 1, tzinfo=pytz.UTC)
AGE_GROUPS = [0, 64, 67, 70, 73, 76, 79, 80, np.Inf]


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


def main():
    sqlContext = get_spark_context()
    quiet_spark_logger(sqlContext.sparkSession)
    sqlContext.sparkSession.conf.set("spark.sql.session.timeZone", "UTC")
    logger = get_logger()

    valid_start = sf.col("start").between(STUDY_START, STUDY_END)
    valid_stop = sf.col("end").between(STUDY_START, STUDY_END)

    logger.info("Reading parameters")
    parameters = read_parameters()
    json_file_path = parameters["path"]
    gender = parameters["gender"]
    bucket_size = parameters["bucket"]
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

    logger.info("Loading cohorts")
    logger.info("Add age information to patients")
    md.get("filter_patients").add_age_information(AGE_REFERENCE_DATE)

    base_cohort = md.get("filter_patients")

    base_cohort.add_age_information()
    for param in subjects_parameters:
        logger.info(param.log())
        base_cohort = param.filter(base_cohort)

    outcomes = md.get("fractures")
    for param in fracture_parameters:
        logger.info(param.log())
        outcomes = param.filter(outcomes)

    followup = md.get("follow_up")
    exposures = md.get("exposures")

    min_base = base_cohort.intersect_all([followup, exposures, outcomes])
    min_base.add_subject_information(base_cohort, "omit_all")
    min_exp = exposures.intersection(min_base)
    min_out = outcomes.intersection(min_base)

    logger.debug("Min base subject count {}".format(min_base.subjects.count()))

    logger.info("Cleaning cohorts")
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
