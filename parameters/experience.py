import json
from typing import Dict, List, Tuple

import pyspark.sql.functions as sf
import pytz

AGE_REFERENCE_DATE = pytz.datetime.datetime(2015, 1, 1, tzinfo=pytz.UTC)

from src.exploration.core.cohort import Cohort
from src.exploration.core.flowchart import Flowchart
from src.exploration.core.io import get_logger
from src.exploration.core.metadata import Metadata

from parameters.exposures_parameters import ControlDrugsParameter
from parameters.followup import CleanFollowUp
from parameters.fractures_parameters import (
    FirstAdmissionOnly,
    FractureSeverityParameter,
    FractureSiteParameter,
    MultiAdmissionParameter,
    MultiFracturedParameter,
)
from parameters.parameter import Parameter
from parameters.patients_parameters import (
    EpilepticsControlParameter,
    GenderParameter,
    OldSubjectsParameter,
)

FOLLOWUP_NAME = "follow_up"
EXPOSURES_NAME = "exposures"
EXTRACT_PATIENTS_NAME = "extract_patients"
FILTER_PATIENTS_NAME = "filter_patients"
FRACTURES_NAME = "fractures"
STUDY_START = pytz.datetime.datetime(2013, 12, 31, 23, 59, 59, tzinfo=pytz.UTC)
STUDY_END = pytz.datetime.datetime(2017, 1, 1, tzinfo=pytz.UTC)


def read_parameters() -> dict:
    with open("parameters.json", "r") as parameters_file:
        parameters_json = "".join(parameters_file.readlines())
        return json.loads(parameters_json)


def read_metadata(file_path: str) -> Metadata:
    with open(file_path, "r") as metadata_file:
        metadata_txt = "".join(metadata_file.readlines())
        return Metadata.from_json(metadata_txt)


def apply_successive_parameters(cohort: Cohort, parameters: List[Parameter]) -> Cohort:
    logger = get_logger()
    new_cohort = cohort
    for parameter in parameters:
        logger.info(parameter.log())
        new_cohort = parameter.filter(new_cohort)
    return new_cohort


def get_the_followup(metadata: Metadata, parameters: Dict) -> Cohort:
    return apply_successive_parameters(
        metadata.get(FOLLOWUP_NAME), get_followup_parameters(parameters, metadata)
    )


def get_subjects(metadata: Metadata, parameters: Dict) -> Cohort:
    return apply_successive_parameters(
        metadata.get(FILTER_PATIENTS_NAME),
        get_patients_parameters(parameters, metadata),
    )


def get_fractures(metadata: Metadata, parameters: Dict) -> Cohort:
    return apply_successive_parameters(
        metadata.get(FRACTURES_NAME), get_fractures_parameters(parameters, metadata)
    )


def get_exposures(metadata: Metadata, parameters: Dict) -> Cohort:
    return apply_successive_parameters(
        metadata.get(EXPOSURES_NAME), get_exposures_parameters(parameters, metadata)
    )


def get_exposures_parameters(parameters: Dict, metadata: Metadata) -> List[Parameter]:
    drugs_control = parameters["drugs_control"]
    drugs_control_param = ControlDrugsParameter(
        drugs_control, metadata.get("control_drugs_exposures")
    )

    return [drugs_control_param]


def get_fractures_parameters(parameters: Dict, metadata: Metadata) -> List[Parameter]:
    site = parameters["site"]
    severity = parameters["fracture_severity"]
    keep_multi_fractured = parameters["keep_multi_fractured"]
    keep_multi_admitted = parameters["keep_multi_admitted"]

    site_param = FractureSiteParameter(site)
    severity_param = FractureSeverityParameter(severity)
    multi_fractured_param = MultiFracturedParameter(keep_multi_fractured)
    multi_admitted_param = MultiAdmissionParameter(keep_multi_admitted)
    first_admission_only = FirstAdmissionOnly()

    return [
        site_param,
        severity_param,
        multi_fractured_param,
        multi_admitted_param,
        first_admission_only,
    ]


def get_followup_parameters(parameters: Dict, metadata: Metadata) -> List[Parameter]:
    valid_start = sf.col("start").between(STUDY_START, STUDY_END)
    valid_stop = sf.col("end").between(STUDY_START, STUDY_END)

    return [CleanFollowUp(valid_start & valid_stop)]


def get_patients_parameters(parameters: Dict, metadata: Metadata) -> List[Parameter]:
    keep_elderly = parameters["keep_elderly"]
    epileptics_control = parameters["epileptics_control"]
    gender = parameters["gender"]

    gender_param = GenderParameter(gender)
    keep_elderly_param = OldSubjectsParameter(keep_elderly)
    epileptics_control_param = EpilepticsControlParameter(
        epileptics_control, metadata.get("epileptics")
    )

    return [gender_param, keep_elderly_param, epileptics_control_param]


def get_cohort_parameters_tuple_list(
    metadata: Metadata, parameters: Dict
) -> List[Tuple[Cohort, List[Parameter]]]:
    cohort_parameters = list()
    cohort_parameters.append(
        (metadata.get(FRACTURES_NAME), get_fractures_parameters(parameters, metadata))
    )

    cohort_parameters.append(
        (metadata.get(EXPOSURES_NAME), get_exposures_parameters(parameters, metadata))
    )

    cohort_parameters.append(
        (
            metadata.get(FILTER_PATIENTS_NAME),
            get_patients_parameters(parameters, metadata),
        )
    )

    return cohort_parameters


def get_initial_flowchart(metadata: Metadata) -> List[Cohort]:
    return [
        metadata.get(EXTRACT_PATIENTS_NAME),
        metadata.get(EXPOSURES_NAME),
        metadata.get(FILTER_PATIENTS_NAME),
        metadata.get(FRACTURES_NAME),
    ]


def add_additional_flowchart_steps(
    initial_flowchart,
    cohort_parameters_list: List[Tuple[Cohort, List[Parameter]]],
    metadata: Metadata,
) -> Tuple[List[Cohort], Metadata]:
    new_metadata = metadata
    for cohort, parameters in cohort_parameters_list:
        for parameter in parameters:
            new_cohort = parameter.filter(cohort)
            get_logger().info(parameter.log())
            get_logger().info("Involved subjects number {}".format(new_cohort.subjects.count()))
            if parameter.change_input:
                initial_flowchart.append(new_cohort)
                new_metadata.add_cohort(new_cohort.name, new_cohort)
                get_logger().info("Adding cohort {} to Flowchart".format(new_cohort.name))

    return initial_flowchart, new_metadata


def experience_to_flowchart_metadata(
    metadata: Metadata, parameters: Dict
) -> Tuple[Flowchart, Metadata]:
    cohort_parameters_list = get_cohort_parameters_tuple_list(metadata, parameters)
    initial_flowchart = get_initial_flowchart(metadata)
    flowchart_steps, new_metadata = add_additional_flowchart_steps(
        initial_flowchart, cohort_parameters_list, metadata
    )
    return Flowchart(initial_flowchart), new_metadata
