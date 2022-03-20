import typing


class LastPipeline(Exception):
    """
    Raise when signal SIGQUIT has been received.
    """
    def __init__(self, settings: typing.Any = None):
        self.__settings = settings

    def settings(self) -> typing.Any:
        return self.__settings


class ExtractEmptyData(Exception):
    """
    Raise when data for handling is empty.
    """
    pass


class LoadFailure(Exception):
    """
    Raise when result in load stage is empty.
    """
    pass


class TransformFailure(Exception):
    """
    Raise for unexpected end of task.
    """

    def __init__(self, message: str = ""):
        self.value = message

    def __str__(self):
        return self.value
