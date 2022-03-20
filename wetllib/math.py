import numpy as np
import nolds
import pandas as pd
from typing import Union, Iterable


def fft(data: pd.Series, size: int) -> tuple:
    """
    Discrete Fourier Transform

    Parameters
    ----------
    data : uniform time series, 1D array
    size : spectral-fftInpLen

    Returns
    -------
    f : frequencies linspace
    ps : power spectral density (according 'f')
    """
    ps = np.abs(np.fft.rfft(data, n=size))
    f = np.linspace(0, 1. / 2, len(ps))
    return f, ps


def generalized_shannon_entropy(data: pd.DataFrame, field: str,
                                               alpha: Union[int, float, None] = None) -> float:
  """
  Parameters:
  -----------
  data : pandas.DataFrame with column '{field}'
  field : column in data for entropy compution
  alpha : parameter for GSE

  -- Attention!!! --
  All values in data[field] must be computed for
  unique parameter set. By these values calculated
  probabilities for parameter set in general dataset.

  Returns:
  --------
  entropy : Generalized Shannon entropy
  """
  from math import log

  if alpha is None:
    raise ValueError("Please send alpha parameter!")

  if data[field].empty:
    raise ValueError("Entropy failed with zero-length array")

  if alpha == 1 or alpha < 0:
    raise ValueError("Bad alpha parameter: {}. Alpha must be greater zero and not equal 1.".format(alpha))

  if len(data[field]) == 1:
    return 0

  data = data[data[field] > 0]

  n = sum(data[field])
  p_i = (data[field] / n) ** alpha
  return (1 / (1 - alpha)) * log(sum(p_i), 2)


def normalized_and_generalized_shannon_entropy(data: pd.DataFrame, field: str,
                                               alpha: Union[int, float, None] = None) -> float:
    """
    See for generalized_shannon_entropy. This normalized result by log N.

    Args:
        data:
        field:
        alpha:

    Returns:
        Normalized GSE.
    """
    from math import log

    return generalized_shannon_entropy(data, field, alpha) / log(sum(data[field]), 2)


def normalized_entropy(sequence: Union[list, Iterable, pd.Series, set], base: int = 2) -> float:
    from scipy.stats import entropy
    from math import log

    return entropy(sequence, base=base) / log(len(set(sequence)), base)


def hurst_change(np_array: np.array) -> float:
    from hurst import compute_Hc
    h_change, _, __ = compute_Hc(np_array, kind='change',
                                 simplified=False)
    return h_change


def hurst_walk(np_array: np.array) -> float:
    from hurst import compute_Hc
    h_walk, _, __ = compute_Hc(np_array, kind='random_walk',
                               simplified=False)
    return h_walk


def corr_dim(np_array: np.array, emb_dim: int = 1) -> float:
    return nolds.corr_dim(np_array, emb_dim=emb_dim)


def lyap_e(np_array: np.array) -> float:
    return nolds.lyap_e(np_array)[0]


def ces(np_array: np.array, sampling_rate: int = 1) -> float:
    from neurokit.signal.complexity import complexity_entropy_spectral
    return complexity_entropy_spectral(np_array, sampling_rate)


def fd_petrosian(np_array: np.array) -> float:
    from neurokit.signal.complexity import complexity_fd_petrosian
    return complexity_fd_petrosian(np_array)


def fd_higushi(np_array: np.array, k_max: int = 128) -> float:
    from neurokit.signal.complexity import complexity_fd_higushi
    return complexity_fd_higushi(np_array, k_max)


def fisher_info(np_array: np.array) -> float:
    from neurokit.signal.complexity import complexity_fisher_info
    return complexity_fisher_info(np_array)


def fboxcox(data: Union[pd.Series, list, np.array]) -> tuple:
    from scipy.stats import boxcox

    transformed, _lambda = boxcox(data)
    if _lambda < 0:
        return np.log1p(data), 0
    return transformed, _lambda


def finv_boxcox(data: Union[list, pd.Series, np.array], _lambda: Union[int, float]) -> Union[pd.Series, np.array, list]:
    from scipy.special import inv_boxcox
    if _lambda == 0:
        return np.expm1(data)
    return inv_boxcox(data, _lambda)
