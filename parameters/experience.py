import json
from typing import Dict, List, Tuple

import pyspark.sql.functions as sf

from scalpel.core.cohort import Cohort
from scalpel.core.cohort_flow import CohortFlow
from scalpel.core.io import get_logger
from scalpel.core.cohort_collection import CohortCollection

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
from parameters.fall_parameters import (
    STUDY_START,
    STUDY_END,
    FRACTURES_NAME,
    EXTRACT_PATIENTS_NAME,
    EXPOSURES_NAME,
    FILTER_PATIENTS_NAME,
    FOLLOWUP_NAME
)


def read_parameters() -> dict:
    with open("parameters.json", "r") as parameters_file:
        parameters_json = "".join(parameters_file.readlines())
        return json.loads(parameters_json)


def read_cohort_collection(file_path: str) -> CohortCollection:
    # TODO: Warning DEPRECATED, please use CohortCollection.from_json(file_path)
    return CohortCollection.from_json(file_path)


def apply_successive_parameters(cohort: Cohort, parameters: List[Parameter]) -> Cohort:
    logger = get_logger()
    new_cohort = cohort
    for parameter in parameters:
        logger.info(parameter.log())
        new_cohort = parameter.filter(new_cohort)
    return new_cohort


def get_the_followup(cc: CohortCollection, parameters: Dict) -> Cohort:
    return apply_successive_parameters(
        cc.get(FOLLOWUP_NAME), get_followup_parameters(parameters, cc)
    )


def get_subjects(cc: CohortCollection, parameters: Dict) -> Cohort:
    return apply_successive_parameters(
        cc.get(FILTER_PATIENTS_NAME), get_patients_parameters(parameters, cc)
    )


def get_fractures(cc: CohortCollection, parameters: Dict) -> Cohort:
    return apply_successive_parameters(
        cc.get(FRACTURES_NAME), get_fractures_parameters(parameters, cc)
    )


def get_exposures(cc: CohortCollection, parameters: Dict) -> Cohort:
    return apply_successive_parameters(
        cc.get(EXPOSURES_NAME), get_exposures_parameters(parameters, cc)
    )


def get_exposures_parameters(parameters: Dict, cc: CohortCollection) -> List[Parameter]:
    drugs_control = parameters["drugs_control"]
    drugs_control_param = ControlDrugsParameter(
        drugs_control, cc.get("control_drugs_exposures")
    )

    return [drugs_control_param]


def get_fractures_parameters(parameters: Dict, cc: CohortCollection) -> List[Parameter]:
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


def get_followup_parameters(parameters: Dict, cc: CohortCollection) -> List[Parameter]:
    valid_start = sf.col("start").between(STUDY_START, STUDY_END)
    valid_stop = sf.col("end").between(STUDY_START, STUDY_END)

    return [CleanFollowUp(valid_start & valid_stop)]


def get_patients_parameters(parameters: Dict, cc: CohortCollection) -> List[Parameter]:
    keep_elderly = parameters["keep_elderly"]
    epileptics_control = parameters["epileptics_control"]
    gender = parameters["gender"]

    gender_param = GenderParameter(gender)
    keep_elderly_param = OldSubjectsParameter(keep_elderly)
    epileptics_control_param = EpilepticsControlParameter(
        epileptics_control, cc.get("epileptics")
    )

    return [gender_param, keep_elderly_param, epileptics_control_param]


def get_cohort_parameters_tuple_list(
    cc: CohortCollection, parameters: Dict
) -> List[Tuple[Cohort, List[Parameter]]]:
    cohort_parameters = list()
    cohort_parameters.append(
        (cc.get(FRACTURES_NAME), get_fractures_parameters(parameters, cc))
    )

    cohort_parameters.append(
        (cc.get(FILTER_PATIENTS_NAME), get_patients_parameters(parameters, cc))
    )

    return cohort_parameters


def get_initial_flowchart(cc: CohortCollection) -> List[Cohort]:
    return [
        cc.get(EXTRACT_PATIENTS_NAME),
        cc.get(EXPOSURES_NAME),
        cc.get(FILTER_PATIENTS_NAME),
        cc.get(FRACTURES_NAME),
    ]


def add_additional_flowchart_steps(
    initial_flowchart,
    cohort_parameters_list: List[Tuple[Cohort, List[Parameter]]],
    cc: CohortCollection,
) -> Tuple[List[Cohort], CohortCollection]:
    new_metadata = cc
    for cohort, parameters in cohort_parameters_list:
        for parameter in parameters:
            new_cohort = parameter.filter(cohort)
            get_logger().info(parameter.log())
            get_logger().info(
                "Involved subjects number {}".format(new_cohort.subjects.count())
            )
            if parameter.change_input:
                initial_flowchart.append(new_cohort)
                new_metadata.add_cohort(new_cohort.name, new_cohort)
                get_logger().info(
                    "Adding cohort {} to CohortFlow".format(new_cohort.name)
                )

    return initial_flowchart, new_metadata


def experience_to_flowchart_metadata(
    cc: CohortCollection, parameters: Dict
) -> Tuple[CohortFlow, CohortCollection]:
    cohort_parameters_list = get_cohort_parameters_tuple_list(cc, parameters)
    initial_flowchart = get_initial_flowchart(cc)
    flowchart_steps, new_metadata = add_additional_flowchart_steps(
        initial_flowchart, cohort_parameters_list, cc
    )
    return CohortFlow(initial_flowchart), new_metadata
