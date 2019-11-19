import pyspark.sql.functions as sf
from pyspark.sql import Window

from src.exploration.core.cohort import Cohort

from parameters.parameter import Parameter


class FractureSiteParameter(Parameter):
    @property
    def change_input(self):
        return self._change_input

    @change_input.setter
    def change_input(self, value):
        self._change_input = value != "all"

    def log(self) -> str:
        return "Fractures sites included: {}".format(self.value)

    def filter(self, fractures: Cohort) -> Cohort:
        if self.value == "all":
            return fractures
        else:
            events = fractures.events.where(sf.col("groupID").isin(self.value))
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


class FractureSeverityParameter(Parameter):
    @property
    def change_input(self):
        return self._change_input

    @change_input.setter
    def change_input(self, value):
        self._change_input = value != "all"

    def log(self) -> str:
        return "Fractures severity included: {}".format(self.value)

    def filter(self, fractures: Cohort) -> Cohort:
        if self.value == "all":
            return fractures
        else:
            self.change_input = True
            events = fractures.events.where(sf.col("weight").isin(self.value))
            if events.count() == 0:
                raise ValueError(
                    "La severite {} n'existe pas dans la cohorte de fractures".format(
                        self.value
                    )
                )
            else:
                return Cohort(
                    "Fractures with severity {}".format(self.value),
                    "Subjects with fractures with severity {}".format(self.value),
                    events.select("patientID").distinct(),
                    events,
                )


class MultiFracturedParameter(Parameter):
    @property
    def change_input(self):
        return self._change_input

    @change_input.setter
    def change_input(self, value):
        self._change_input = not value

    def log(self) -> str:
        return "Keep multi fractured: {}".format(self.value)

    def filter(self, cohort: Cohort) -> Cohort:
        if self.value:
            return cohort
        else:
            self.change_input = True
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
                "Fractured once per admission",
                "{} that has only one fracture per admission".format(
                    cohort.characteristics
                ),
                new_fractures.select("patientID").distinct(),
                new_fractures,
            )


class MultiAdmissionParameter(Parameter):
    @property
    def change_input(self):
        return self._change_input

    @change_input.setter
    def change_input(self, value):
        self._change_input = not value

    def log(self) -> str:
        return "Keep multi admitted: {}".format(self.value)

    def filter(self, cohort: Cohort) -> Cohort:
        if self.value:
            return cohort
        else:
            self.change_input = True
            admitted_once = (
                cohort.events.groupBy(["patientID", "start"])
                .count()
                .groupby("patientID")
                .count()
                .where(sf.col("count") == 1)
            )
            new_fractures = cohort.events.join(admitted_once, "patientID")
            return Cohort(
                "Admitted once fractures",
                "{} that has only one admission for fracture".format(
                    cohort.characteristics
                ),
                new_fractures.select("patientID").distinct(),
                new_fractures,
            )


class FirstAdmissionOnly(Parameter):
    def __init__(self):
        super().__init__(None)

    @property
    def change_input(self):
        return self._change_input

    @change_input.setter
    def change_input(self, value):
        self._change_input = True

    def log(self) -> str:
        return "Keep only first admission. This is due to problems with Cross Validation."

    def filter(self, fractures: Cohort) -> Cohort:
        window = Window.partitionBy("patientID").orderBy("start")
        events = (
            fractures.events.withColumn("rn", sf.row_number().over(window))
            .where(sf.col("rn") == 1)
            .drop("rn")
        )
        return Cohort(
            "First admission fractures only",
            "Subjects with first admission fractures",
            events.select("patientID").distinct(),
            events,
        )
