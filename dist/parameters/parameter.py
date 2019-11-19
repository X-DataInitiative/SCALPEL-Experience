import abc
from src.exploration.core.cohort import Cohort


class Parameter(abc.ABC):
    def __init__(self, value):
        self._value = None
        self.value = value
        self._change_input = None
        self.change_input = value

    @property
    def value(self) -> None:
        return self._value

    @value.setter
    def value(self, value):
        self._value = value

    @property
    @abc.abstractmethod
    def change_input(self) -> None:
        return self._change_input

    @change_input.setter
    @abc.abstractmethod
    def change_input(self, value):
        pass

    @abc.abstractmethod
    def log(self) -> str:
        """
        Logs the current parameter filtering
        :return: str
        """

    @abc.abstractmethod
    def filter(self, cohort: Cohort) -> Cohort:
        """Applies the parameter based on the passed value"""
