"""
Test Arrow and Pandas compatibility for the NumpyExtArray objects. 
"""

import pytest
import pyarrow as pa
import pandas as pd
import numpy as np

from arrowpy.arrow_ext import ArrowNumpyExtArray, to_numpy_array
from arrowpy.io import ArrowTable


def test_numpy_trip():
    _arr = np.arange(1000_00, dtype=np.float).reshape(10000, 5, 2)
    arr = ArrowNumpyExtArray.from_numpy(_arr)
    assert (to_numpy_array(arr) == _arr).all(), "Sadly, the numpy array didn't survive the trip to Arrow"

    _bool_arr = np.ones((5000, 2, 2), dtype=np.bool)
    bool_arr = ArrowNumpyExtArray.from_numpy(_bool_arr)
    assert (to_numpy_array(bool_arr) == _bool_arr).all(), "Sadly, the boolean numpy array didn't survive the trip to Arrow"
    

def test_pandas_roundtrip():
    _arr = np.arange(1000_00).reshape(10000, 5, 2)
    arr = ArrowNumpyExtArray.from_numpy(_arr)
    barr = np.arange(10000)
    table = pa.Table.from_arrays([arr, barr], names=['colA', 'colB'])

    df = table.to_pandas()
    new_table = pa.table(df)
    arr2 = new_table.column('colA')

    assert (to_numpy_array(arr) == to_numpy_array(arr2)).all(), "Sadly, the Arrow Extension array didn't surive the trip to pandas"


def test_matching_metadata():
    """check that the metadata in the extension fields is copied successfully to the schema"""
    sz = 1000_000
    arr = np.arange(sz * 10, dtype=np.int32).reshape(sz, 5, 2)
    barr = np.arange(sz)
    table_ext = ArrowTable.from_numpy_arrays([arr, barr], names=['colA', 'colB'])
    
    ext_column = table_ext.data.column('colA')
    if isinstance(ext_column, pa.ChunkedArray):
        ext_column = ext_column.chunk(0)
    _table_noext = pa.Table.from_arrays([ext_column.storage, pa.array(barr)], names=['colA', 'colB'])
    new_schema = _table_noext.schema.with_metadata(table_ext.schema.metadata)
    table_noext = ArrowTable(_table_noext.cast(new_schema))

    recv_column = table_noext.data.column('colA')
    if isinstance(recv_column, pa.ChunkedArray):
        recv_column = recv_column.chunk(0)
    assert isinstance(recv_column, ArrowNumpyExtArray), "Expected `colA` to be recognized as an extension type"
