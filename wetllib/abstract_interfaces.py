from abc import ABC, abstractmethod
from collections.abc import Callable


class AbstractTask(ABC, Callable):
    @abstractmethod
    def extract(self, **kwargs):
        """
        Define extract method for you Task
        """

    @abstractmethod
    def transform(self, **kwargs):
        """
        Define transform method for you Task
        """

    @abstractmethod
    def load(self, **kwargs):
        """
        Define load method for you Task
        """

    @abstractmethod
    def __call__(self, **kwargs):
        """
        Define call method for you Task
        """


class AbstractEvent(ABC, Callable):
    @abstractmethod
    def __call__(self, **kwargs):
        """
        Define call method for you Event
        """

    @abstractmethod
    def handler(self, **kwargs):
        """
        Define handler method for you Event
        """
