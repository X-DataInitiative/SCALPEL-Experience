from src.exploration.core.cohort import Cohort

from parameters.parameter import Parameter


class CleanFollowUp(Parameter):
    @property
    def change_input(self):
        return self._change_input

    @change_input.setter
    def change_input(self, value):
        self._change_input = True

    def log(self) -> str:
        return "Removing subjects with FollowUp outside studies."

    def filter(self, follow_up: Cohort) -> Cohort:
        clean = follow_up.events.where(self.value)
        return Cohort(
            "FollowUp within study bounds.",
            "Subjects with FollowUp with study bounds",
            clean.select("patientID").distinct(),
            clean,
        )
