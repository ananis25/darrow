"""
Copied from the pandas test suite to test the numpy extension arrays. 
Things we leave out: 
* Array with missing values - we work with dense numpy arrays, subarrays are never NA. 
* Array with unique/ranked values - numpy subarrays are not hashable or have a sort order. 
"""

import operator

import pytest
import numpy as np
import pyarrow as pa
import pandas as pd
from pandas import Series

from arrowpy.pandas_ext import NumpyExtType, NumpyExtArray


@pytest.fixture
def dtype():
    """A fixture providing the ExtensionDtype to validate."""
    sub_shape = (5,4)
    return NumpyExtType(sub_shape, np.dtype('int64'))


@pytest.fixture
def data():
    """
    Length-100 array for this type.
    * data[0] and data[1] should both be non missing
    * data[0] and data[1] should not be equal
    """
    sub_shape = (5,4)
    return NumpyExtArray(np.arange(100 * np.prod(sub_shape)).reshape(100, *sub_shape))


@pytest.fixture
def data_missing():
    raise NotImplementedError("NumpyExtArray doesn't have missing elements")


@pytest.fixture
def data_for_twos():
    """Length-100 array in which all the elements are two."""
    raise NotImplementedError


@pytest.fixture(params=["data"])
def all_data(request, data):
    """Parametrized fixture giving 'data' and 'data_missing'
    We left out `data_missing`. 
    """
    return data


@pytest.fixture
def data_repeated(data):
    """
    Generate many datasets.
    Parameters
    ----------
    data : fixture implementing `data`
    Returns
    -------
    Callable[[int], Generator]:
        A callable that takes a `count` argument and
        returns a generator yielding `count` datasets.
    """

    def gen(count):
        for _ in range(count):
            yield data

    return gen


@pytest.fixture
def data_for_sorting():
    """
    Length-3 array with a known sort order.
    This should be three items [B, C, A] with
    A < B < C
    """
    raise NotImplementedError


@pytest.fixture
def data_missing_for_sorting():
    """
    Length-3 array with a known sort order.
    This should be three items [B, NA, A] with
    A < B and NA missing.
    """
    raise NotImplementedError


@pytest.fixture
def na_cmp():
    """
    Binary operator for comparing NA values.
    Should return a function of two arguments that returns
    True if both arguments are (scalar) NA for your type.
    By default, uses ``operator.is_``
    """
    return NotImplementedError


@pytest.fixture
def na_value(dtype):
    """The scalar missing value for this type. Default 'None'"""
    return dtype.na_value


@pytest.fixture
def data_for_grouping():
    """
    Data for factorization, grouping, and unique tests.
    Expected to be like [B, B, NA, NA, A, A, B, C]
    Where A < B < C and NA is missing
    """
    raise NotImplementedError


@pytest.fixture(params=[True, False])
def box_in_series(request):
    """Whether to box the data in a Series"""
    return request.param


@pytest.fixture(
    params=[
        lambda x: 1,
        lambda x: [1] * len(x),
        lambda x: Series([1] * len(x)),
        lambda x: x,
    ],
    ids=["scalar", "list", "series", "object"],
)
def groupby_apply_op(request):
    """
    Functions to test groupby.apply().
    """
    return request.param


@pytest.fixture(params=[True, False])
def as_frame(request):
    """
    Boolean fixture to support Series and Series.to_frame() comparison testing.
    """
    return request.param


@pytest.fixture(params=[True, False])
def as_series(request):
    """
    Boolean fixture to support arr and Series(arr) comparison testing.
    """
    return request.param


@pytest.fixture(params=[True, False])
def use_numpy(request):
    """
    Boolean fixture to support comparison testing of ExtensionDtype array
    and numpy array.
    """
    return request.param


@pytest.fixture(params=["ffill", "bfill"])
def fillna_method(request):
    """
    Parametrized fixture giving method parameters 'ffill' and 'bfill' for
    Series.fillna(method=<method>) testing.
    """
    return request.param


@pytest.fixture(params=[True, False])
def as_array(request):
    """
    Boolean fixture to support ExtensionDtype _from_sequence method testing.
    """
    return request.param
