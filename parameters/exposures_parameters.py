from src.exploration.core.cohort import Cohort

from parameters.parameter import Parameter


class ControlDrugsParameter(Parameter):
    def __init__(self, value, control_drugs: Cohort):
        super().__init__(value)
        self.control_drugs = control_drugs

    @property
    def change_input(self):
        return self._change_input

    @change_input.setter
    def change_input(self, value):
        self._change_input = value

    def log(self) -> str:
        return "Include control drugs : {}".format(self.value)

    def filter(self, cohort: Cohort) -> Cohort:
        if self.value:
            control_drugs_exposures = self.control_drugs.events.join(
                cohort.subjects, "patientID", "inner"
            )
            return Cohort(
                cohort.name,
                cohort.characteristics,
                cohort.subjects,
                cohort.events.union(control_drugs_exposures),
            )
        else:
            return cohort
