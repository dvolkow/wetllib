import datetime as dt
from time import sleep
from typing import Union
from collections.abc import Iterable


STARTF = '%Y-%m-%d %H:%M:00'
ENDF = '%Y-%m-%d %H:%M:59'
STDF = '%Y-%m-%d %H:%M:%S'
DATEF = '%Y-%m-%d'


def _to_str(time: Union[str, dt.datetime], fmt: str = STDF) -> str:
    if isinstance(time, str):
        return time
    return dt.datetime.strftime(time, fmt)


def _to_time(string: Union[str, dt.datetime], fmt: str = STDF) -> dt.datetime:
    if isinstance(string, str):
        return dt.datetime.strptime(string, fmt)
    return string


class TimeProducer(Iterable):
    def __init__(self, period: int, delay: int, length: Union[int, None] = None, fast_start: bool = False):
        if length is None:
            length = period

        if not fast_start:
            self.start = _to_time(_to_str(dt.datetime.now() - dt.timedelta(minutes=delay), STARTF))
        else:
            self.start = _to_time(_to_str(dt.datetime.now() - dt.timedelta(minutes=delay + length - 1), STARTF))

        self.period = period
        self.delay = delay
        self.length = length

    def __iter__(self):
        while True:
            yield _to_str(self.start), _to_str(self.start + dt.timedelta(minutes=self.length - 1), ENDF)
            self.start += dt.timedelta(minutes=self.period)

    def _allowed(self) -> bool:
        return dt.datetime.now() > self.start + dt.timedelta(minutes=self.length - 1 + self.delay)

    def limited(self, n: int):
        counter = 0
        for time in self:
            yield time
            counter += 1
            if counter == n:
                break

    def periodic(self):
        for time in self:
            while not self._allowed():
                sleep(30)
            yield time
