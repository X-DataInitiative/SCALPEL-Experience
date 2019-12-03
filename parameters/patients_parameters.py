from scalpel.core.cohort import Cohort

from parameters.experience import AGE_REFERENCE_DATE
from parameters.parameter import Parameter
import pyspark.sql.functions as sf


class GenderParameter(Parameter):
    @property
    def change_input(self):
        return self._change_input

    @change_input.setter
    def change_input(self, value):
        self._change_input = not (value == "all")

    def log(self):
        return "Genders included: {}".format(self.value)

    def filter(self, cohort: Cohort) -> Cohort:
        if self.value == "homme":
            return Cohort(
                "Male",
                "Male subjects",
                cohort.subjects.where(sf.col("gender") == 1),
                None,
            )
        elif self.value == "femme":
            return Cohort(
                "Female",
                "Female subjects",
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
    @property
    def change_input(self):
        return self._change_input

    @change_input.setter
    def change_input(self, value):
        self._change_input = not value

    def log(self) -> str:
        return "Keep old subjects filter: {}".format(self.value)

    def filter(self, cohort: Cohort) -> Cohort:
        if self.value is True:
            return cohort
        else:
            cohort.add_age_information(AGE_REFERENCE_DATE)
            young_subjects = cohort.subjects.where(sf.col("age") < 85)
            return Cohort("Young subjects", "Young subjects", young_subjects, None)


class EpilepticsControlParameter(Parameter):
    def __init__(self, value, epileptics: Cohort):
        super().__init__(value)
        self.epileptics = epileptics

    @property
    def change_input(self):
        return self._change_input

    @change_input.setter
    def change_input(self, value):
        self._change_input = value

    def log(self) -> str:
        return "Exclude Epileptics: {}".format(self.value)

    def filter(self, cohort: Cohort) -> Cohort:
        if self.value:
            new_cohort = cohort.difference(self.epileptics)
            new_cohort.name = "{} without Epileptics".format(cohort.name)
            return new_cohort
        else:
            return cohort
