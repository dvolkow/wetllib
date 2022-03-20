"""
Subroutines and functions for preprocessing your data for analysis.
"""
import numpy as np
import pandas as pd
import typing
from ipaddress import IPv6Address, IPv4Address, AddressValueError


class Splitter:
    """
    Methods for split your dataset.
    Mode:
        - by_index: split to equal parts by records count
    All methods returns list with parts of dataset.
    """
    def __init__(self, mode: str = "by_index"):
        self._call_map = {"by_index": Splitter._by_index}

        if mode not in self._call_map.keys():
            raise TypeError("Splitter: Mode {} is not supporting."
                            "".format(mode))
        self._mode = mode

    def __call__(self, data: pd.Series, n_parts: int) -> list:
        """
        Parameters:
        -----------
        data : Time Series, datetime indecies.
        n_parts : number of parts
        """
        return self._call_map[self._mode](data, n_parts)

    @staticmethod
    def _by_index(data: pd.Series, n_parts: int) -> list:
        return np.array_split(data, n_parts)


class Resampler:
    """
    Transform non-uniform time series to uniform time series
    """
    def __init__(self, mode: str = "counter"):
        self._cmap = dict(counter=Resampler._counter, mean=Resampler._mean, median=Resampler._median,
                          var=Resampler._var)

        if mode not in self._cmap.keys():
            raise TypeError('Resample: Mode {} is '
                            'not supporting.'.format(mode))
        self._mode = mode

    def __call__(self, data: pd.Series, period: str) -> pd.Series:
        return self._cmap[self._mode](data, period)

    @staticmethod
    def _counter(data: pd.Series, period: str) -> pd.Series:
        return pd.Series(data).resample(period).sum()

    @staticmethod
    def _mean(data: pd.Series, period: str) -> pd.Series:
        return pd.Series(data).resample(period).mean().fillna(0)

    @staticmethod
    def _median(data: pd.Series, period: str) -> pd.Series:
        return pd.Series(data).resample(period).median().fillna(0)

    @staticmethod
    def _var(data: pd.Series, period: str) -> pd.Series:
        return pd.Series(data).resample(period).var().fillna(0)


def ipv4_mapped_str_from_ipv6(string: str) -> typing.Union[IPv4Address, str]:
    try:
        IPv4Address(string)
    except AddressValueError:
        return str(IPv6Address(string).ipv4_mapped)
    return string


def default_reduce(data: dict) -> pd.DataFrame:
    """
    Parameters:
    -----------
    data : Manager.dict

    Returns:
    --------
    res : list of ip recommended to ban
    """
    from wetllib.adapters import concat

    df_list = []
    for i in data.keys():
        df_list.append(data[i])

    return concat(df_list)
