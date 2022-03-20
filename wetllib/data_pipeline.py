from abc import ABC
from wetllib.abstract_interfaces import AbstractTask
from wetllib.metadata import TaskMetaDataRecord


class DataPipelineTask(AbstractTask, ABC):
    """
    Define 'extract', 'transform' and 'load' methods into your data pipeline.
    Note:
        'extract' must put data from database into self.data
        'transform' must put transformed_data into self.transformed_data
        'load' must load self.transformed_data to database
    """
    def __init__(self):
        self.data = None
        self.transformed_data = None

    def __call__(self, **kwargs) -> None:
        self.metadata = self.init_metadata(**kwargs)
        self.data = self.extract(**kwargs)
        self.transformed_data = self.transform(**kwargs)
        self.load(**kwargs)
        self.send_metadata(**kwargs)

    def send_metadata(self, **kwargs) -> None:
        """
        Define send_metadata method for you Task
        """
        pass

    def init_metadata(self, **kwargs) -> TaskMetaDataRecord:
        """
        Define init_metadata method for you Task
        """
        pass
