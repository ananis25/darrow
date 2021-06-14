"""
This module implements a pandas extension datatype/array wrapping multidim numpy arrays. 
The default pandas behavior is to convert it to a numpy array of type `object`, which is 
not the most efficient. For ex - an arrow dataset of size 500MB with some multidim arrays 
as columns explodes to 3GB. All I want is to filter on other column values, till an arrow 
native dataframe library comes along! 

Even so, a lot of other libraries require a pandas dataframe as input, so this gives us 
efficient conversion. 
"""

import json
import logging
from typing import Any, Tuple, Type, Sequence, List, Optional, Union

import numbers
import numpy as np
import pandas as pd
import pyarrow as pa
from pandas.api.extensions import ExtensionDtype, ExtensionArray

logger = logging.getLogger(__name__)


# Without boxing values like this, we'd get a different object indexing into the NumpyExtArray
# each time and fail extension array tests. I want to pass that test suite.
#     ```
#     x = np.arange(20).reshape(5, 2, 2)
#     lx = list(x); ex = NumpyExtArray(x)
#     print(id(lx[0])); print(id(ex[0]))
#     print(id(lx[0])); print(id(ex[0]))
#     ```
class NumpyExtScalar:
    """
    Pandas expects series elements to be scalars, NumpyExtArray returns references to subarrays.
    This class boxes a numpy subarray, but isn't meant to be array-like.

    Use the :attr: data if you want the underlying numpy array.
    """

    data: np.ndarray

    def __init__(self, array: np.ndarray, copy: bool = False):
        assert isinstance(
            array, np.ndarray
        ), f"NumpyExtScalar expected a numpy array, got {type(array)}"
        self.data = array if not copy else np.array(array)

    def __repr__(self):
        return repr(self.data)

    def __eq__(self, other: "NumpyExtScalar") -> bool:
        assert isinstance(
            other, NumpyExtScalar
        ), f"eq comparison expects a NumpyExtScalar, got {type(other)}"
        assert self.data.shape == other.data.shape
        return (self.data == other.data).all()

    def __hash__(self) -> int:
        """This is not great and only checks the string repr of the array but we'll see how far can we get"""
        return hash(str(self.data))


class NumpyExtType(ExtensionDtype):
    """
    Pandas describes the interface at: https://github.com/pandas-dev/pandas/blob/master/pandas/core/dtypes/base.py

    * Earlier written using the pandas documentation initially, but turned out very similar to the Huggingface
    Datasets library implementation (read as `copied`), at - https://github.com/huggingface/datasets/blob/master/src/datasets/features.py
    * Another useful source is: https://github.com/ContinuumIO/cyberpandas/
    """

    _sub_shape: Tuple[int, ...]
    _dtype: np.dtype
    _metadata: Tuple = ("ndtype", "sub_shape")

    def __init__(self, sub_shape: Tuple[int, ...], ndtype: np.dtype):
        assert isinstance(
            ndtype, np.dtype
        ), f"Expected `ndtype` of type `np.dtype`, received {type(ndtype)}"
        self._sub_shape = tuple(sub_shape)
        self._dtype = ndtype

    def __from_arrow__(
        self, array: Union[pa.ExtensionArray, pa.ChunkedArray]
    ) -> "NumpyExtArray":
        from .arrow_ext import ArrowNumpyExtType

        assert isinstance(
            array.type, ArrowNumpyExtType
        ), f"Expected array of type ArrowNumpyExtType, received: {type(array)}"
        if isinstance(array, pa.ChunkedArray):
            return NumpyExtArray(
                np.vstack([chunk.to_numpy() for chunk in array.chunks])
            )
        else:
            return NumpyExtArray(array.to_numpy(array))

    def __str__(self):
        # pandas has trouble with commas (wtf), we'll serialize the type as: NumpyExtType:int:4:5
        return f"NumpyExtType:{str(self.ndtype)}:{':'.join(str(x) for x in self.sub_shape)}"

    @classmethod
    def construct_from_string(cls, string: str):
        if not isinstance(string, str):
            raise TypeError(
                f"'construct_from_string' expects a string, got {type(string)}"
            )
        try:
            splits = string.split(":")
            return cls(tuple(int(x) for x in splits[2:]), np.dtype(splits[1]))
        except:
            raise TypeError(f"Cannot construct a 'NumpyExtType' from '{string}'")

    @classmethod
    def construct_array_type(cls) -> Type["ExtensionArray"]:
        return NumpyExtArray

    @property
    def sub_shape(self):
        return self._sub_shape

    @property
    def ndtype(self):
        return self._dtype

    @property
    def type(self):
        return NumpyExtScalar

    @property
    def na_value(self):
        return NumpyExtScalar(np.zeros(self._sub_shape, dtype=self.ndtype))

    @property
    def name(self) -> str:
        return str(self)

    @property
    def _is_boolean(self) -> bool:
        return self.ndtype == np.dtype("bool")

    @property
    def _is_numeric(self) -> bool:
        return np.issubdtype(self.ndtype, np.number)


class NumpyExtArray(ExtensionArray, np.lib.mixins.NDArrayOperatorsMixin):
    """
    Pandas describes the interface at: https://github.com/pandas-dev/pandas/blob/master/pandas/core/arrays/base.py
    """

    data: np.ndarray
    _ext_type: "NumpyExtType"
    _HANDLED_TYPES = (np.ndarray, numbers.Number)

    def __init__(self, array: np.ndarray, copy: bool = False):
        assert (
            array.ndim > 1
        ), "Pandas extension array is meant only for multidim numpy arrays"
        self.data = array if not copy else np.array(array)
        self._ext_type = NumpyExtType(array.shape[1:], array.dtype)

    def __arrow_array__(self, type=None):
        """The backing numpy array should get serialized to an Arrow extension array for numpy tensors"""
        from .arrow_ext import ArrowNumpyExtArray

        return ArrowNumpyExtArray.from_numpy(self.data)

    def __getitem__(self, i) -> Union["NumpyExtArray", "NumpyExtScalar"]:
        """pass on to the underlying array"""
        if not isinstance(i, (int, slice, np.ndarray)):
            raise NotImplementedError("Unexpected input for indexing")
        if np.isscalar(i):
            return NumpyExtScalar(self.data[i])
        else:
            return type(self)(self.data[i])

    def __setitem__(self, key: Union[int, slice, np.ndarray], value: Any) -> None:
        """We want immutable arrays in the pandas dataframe.
        TODO: do we let arrays be mutated?
        """
        raise NotImplementedError(
            "Contact the maintainers. We want dataframes to be immutable"
        )

    def __eq__(self, other: "NumpyExtArray") -> np.ndarray:
        if not isinstance(other, NumpyExtArray):
            return NotImplemented
        else:
            assert self.dtype == other.dtype
            assert self.data.shape == other.data.shape
            return np.logical_and.reduce(
                self.data == other.data, tuple(range(1, self.data.ndim))
            )

    def __len__(self):
        return len(self.data)

    @property
    def dtype(self) -> "NumpyExtType":
        return self._ext_type

    @property
    def nbytes(self) -> int:
        return self.data.nbytes

    @classmethod
    def _concat_same_type(cls, to_concat: Sequence["NumpyExtArray"]) -> "NumpyExtArray":
        return cls(np.vstack([x.data for x in to_concat]))

    @classmethod
    def _from_sequence(cls, scalars, *, dtype=None, copy=False) -> "NumpyExtArray":
        if isinstance(scalars, NumpyExtArray):
            return cls(scalars.data, copy=copy)
        if isinstance(scalars, NumpyExtScalar):
            # pandas shouldn't be passing this, but series construction with empty data does: `pd.Series(index=[1, 2, 3], dtype=NumpyExtType((2,4), np.dtype('int')))``
            # Their fix (https://github.com/pandas-dev/pandas/pull/33846) assumes scalars are always one of the actual scalar types, not objects
            scalars = [scalars]
        elif len(scalars) == 0:
            scalars = [dtype.na_value]
        return cls(np.stack([x.data for x in scalars], axis=0))

    @classmethod
    def _from_factorized(cls, values, original):
        """TODO: should you implement hashing for numpy arrays?"""
        raise NotImplementedError(
            "Pandas factorization is not defined for this extension type"
        )

    def astype(self, dtype, copy=True):
        if isinstance(dtype, NumpyExtType):
            if dtype == self.dtype:
                return self.copy() if copy else self
            else:
                return type(self)(self.data.astype(dtype.ndtype, copy=copy))

        logger.warning("Avoid triggering a full array conversion for `NumpyExtArray`")
        if dtype in (object, str):
            return np.array(self, dtype=dtype)
        return super().astype(dtype)

    def copy(self):
        return type(self)(self.data, copy=True)

    def isna(self) -> np.ndarray:
        """We are always working with dense arrays"""
        return np.zeros(len(self), dtype=np.bool)

    def take(
        self,
        indices: Sequence[int],
        *,
        allow_fill: bool = False,
        fill_value: Any = None,
    ) -> "NumpyExtArray":
        """
        Copied off the cyberpandas implementation. I don't get what pandas is trying to do here.

        Link: https://github.com/ContinuumIO/cyberpandas/blob/master/cyberpandas/ip_array.py
        """
        indices = np.asarray(indices, dtype="int")
        mask = indices == -1

        if allow_fill:
            if len(self) == 0:
                if not (indices == -1).all():
                    raise IndexError("Invalid take for empty array. Must be all -1.")
                else:
                    # return an array with all NA values
                    fill_value = self.dtype.na_value.data
                    return type(self)(np.stack([fill_value] * len(indices), axis=0))
            else:
                if (indices < -1).any():
                    raise ValueError(
                        "Invalid value in 'indices'. Must be all >= -1 for 'allow_fill=True'."
                    )

        took = np.take(self.data, indices, axis=0)
        if allow_fill:
            took[mask] = self.dtype.na_value.data
            if mask.any():
                logger.warning(
                    "Pandas might have inserted zero subarrays for some indices, tread carefully"
                )
        return type(self)(took, copy=False)

    def __array_ufunc__(self, ufunc, method, *inputs, **kwargs) -> Any:
        """
        Copied off the numpy [documentation](https://numpy.org/doc/stable/reference/generated/numpy.lib.mixins.NDArrayOperatorsMixin.html).
        I can follow what they're doing.
        """
        out = kwargs.get("out", ())
        for x in inputs + out:
            if not isinstance(x, self._HANDLED_TYPES + (NumpyExtArray,)):
                return NotImplemented

        inputs = tuple(x.data if isinstance(x, NumpyExtArray) else x for x in inputs)
        if out:
            kwargs["out"] = tuple(
                x.data if isinstance(x, NumpyExtArray) else x for x in out
            )
        result = getattr(ufunc, method)(*inputs, **kwargs)

        if type(result) is tuple:
            return tuple(type(self)(x) for x in result)
        elif method == "at":
            return None
        else:
            return type(self)(result)


@pd.api.extensions.register_dataframe_accessor("ts")
class TsAccessor:
    def __init__(self, pandas_obj):
        assert isinstance(pandas_obj, pd.DataFrame)
        self._obj = pandas_obj

    def project(self, columns: List[str]) -> pd.DataFrame:
        """lack of this in pandas api really irks me"""
        return self._obj[columns]

    def npcol(self, column: str) -> np.ndarray:
        """get the pandas column as a numpy array"""
        series = self._obj[column]
        return (
            series.array.data
            if isinstance(series.array, NumpyExtArray)
            else series.to_numpy()
        )


# register the extension types for Pandas
pd.api.extensions.register_extension_dtype(NumpyExtType)

__all__ = ["NumpyExtScalar", "NumpyExtType", "NumpyExtArray"]