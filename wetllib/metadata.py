import datetime as dt
import pandas as pd
import typing
from socket import gethostname
from wetllib.scheduler import _to_time


class TaskMetaDataRecord:
    def __init__(self, expert: str, instance_name: str, data_times: list):
        self.data_times = data_times
        self.instance_times = dt.datetime.now()
        self.hostname = gethostname()
        self.expert = expert
        self.instance_name = instance_name
        self.duration = dict(extract=0, transform=0, load=0)
        self.events = 0
        self.status = ""
        self.addition = ""

    @property
    def to_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame([{'start_data_times': _to_time(self.data_times[0]),
                              'end_data_times': _to_time(self.data_times[1]),
                              'start_instance_times': self.instance_times,
                              'end_instance_times': dt.datetime.now(),
                              'hostname': self.hostname,
                              'expert': self.expert,
                              'instance_name': self.instance_name,
                              'extract_duration': self.duration['extract'],
                              'transform_duration': self.duration['transform'],
                              'load_duration': self.duration['load'],
                              'events': self.events,
                              'status': self.status,
                              'addition': self.addition}])


def duration_decorator(f: typing.Callable):
    """
    Useful for measurement of performance individual instances.
    Your class must provide .metadata property\attribute.
    """
    def wrapper(*args, **kw):
        start = dt.datetime.now()
        result = f(*args, **kw)
        duration = int((dt.datetime.now() - start).total_seconds())
        args[0].metadata.duration[f.__name__] = duration  # self is first argument in class method
        return result
    return wrapper

