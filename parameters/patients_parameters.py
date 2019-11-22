from src.exploration.core.cohort import Cohort
from src.exploration.core.metadata import Metadata

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
            young_subjects = cohort.subjects.where(sf.col("age") < 85)
            return Cohort("Young subjects", "Young subjects", young_subjects, None)


class EpilepticsControlParameter(Parameter):
    def __init__(self, value, md: Metadata):
        super().__init__(value)
        self.md = md

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
            epileptics = self.md.get('epileptics')
            new_cohort = cohort.difference(epileptics)
            new_cohort.name = epileptics.name
            return new_cohort
        else:
            return cohort
