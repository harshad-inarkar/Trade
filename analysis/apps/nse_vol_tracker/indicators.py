"""
indicators.py
-------------
Strict, vectorised indicator calculations using NumPy.
"""

import numpy as np


class IndicatorFactory:
    """Calculates moving averages dynamically over 1D or 2D NumPy arrays."""

    @staticmethod
    def sma(arr: np.ndarray, period: int) -> np.ndarray:
        if arr.shape[-1] < period:
            return np.zeros_like(arr)

        res = np.zeros_like(arr, dtype=np.float64)
        cs = np.cumsum(arr, axis=-1)

        if arr.ndim == 2:
            res[:, period - 1] = cs[:, period - 1] / period
            res[:, period:] = (cs[:, period:] - cs[:, :-period]) / period
        else:
            res[period - 1] = cs[period - 1] / period
            res[period:] = (cs[period:] - cs[:-period]) / period

        return res

    @staticmethod
    def _calc_exponential_ma(arr: np.ndarray, alpha: float) -> np.ndarray:
        """Common recursive formula for EMA, RMA, and SMMA."""
        res = np.zeros_like(arr, dtype=np.float64)

        if arr.ndim == 2:
            res[:, 0] = arr[:, 0]
            for t in range(1, arr.shape[1]):
                res[:, t] = alpha * arr[:, t] + (1 - alpha) * res[:, t - 1]
        else:
            res[0] = arr[0]
            for t in range(1, arr.shape[0]):
                res[t] = alpha * arr[t] + (1 - alpha) * res[t - 1]

        return res

    @classmethod
    def ema(cls, arr: np.ndarray, period: int) -> np.ndarray:
        alpha = 2.0 / (period + 1)
        return cls._calc_exponential_ma(arr, alpha)

    @classmethod
    def rma(cls, arr: np.ndarray, period: int) -> np.ndarray:
        alpha = 1.0 / period
        return cls._calc_exponential_ma(arr, alpha)

    @classmethod
    def calculate(cls, ma_type: str, arr: np.ndarray, period: int) -> np.ndarray:
        ma_type = ma_type.lower()
        if ma_type == "sma":
            return cls.sma(arr, period)
        if ma_type == "ema":
            return cls.ema(arr, period)
        # Default to RMA if unmapped
        return cls.rma(arr, period)
