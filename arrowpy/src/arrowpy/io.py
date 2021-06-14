"""
Helper classes to read off an Arrow IPC format file, either as a memory mapped table or a stream 
of record batches. 

NOTE<dev>: The feather format is the same as the Arrow file format per the documentation, though 
the feather writer doesn't quite work correctly with compression, producing a file that can't be 
read by Arrow IPC reader or decompressed using the zstd cli. 
"""

import json
import logging
from enum import Enum
from typing import Iterator, Dict, List, Tuple, Optional, Union

import pyarrow as pa
import pyarrow.compute as pc
import pandas as pd
import numpy as np

from .arrow_ext import ArrowNumpyExtType, ArrowNumpyExtArray, to_numpy_array


logger = logging.getLogger(__name__)

__all__ = ["ArrowTable", "ArrowWriter", "ArrowReader"]


def _arrow_to_pandas_type(_type):
    if pa.types.is_string(_type):
        return pd.StringDtype()


class ArrowTable:
    """
    Abstraction for both a pyarrow record batch or a table; provides easy column
    accessors and conversion to a pandas dataframe.

    NOTE<dev>: A wropper class is also needed since Arrow files produced by the D library don't
    support extension types until the C bindings don't allow adding field metadata. Hence, we
    keep a copy of the extension type info in the schema metadata dictionary.
    """

    _data: Union[pa.RecordBatch, pa.Table]

    def __init__(self, data: Union[pa.RecordBatch, pa.Table]):
        self._data = data

    @classmethod
    def from_numpy_arrays(
        cls, arrays: List[np.ndarray], names: List[str]
    ) -> "ArrowTable":
        """Create an ArrowTable from a list of numpy arrays"""
        assert all(
            isinstance(col, np.ndarray) for col in arrays
        ), "Expected column inputs to be numpy arrays"
        list_arrays = [
            ArrowNumpyExtArray.from_numpy(array) if array.ndim > 1 else pa.array(array)
            for array in arrays
        ]
        return cls(pa.Table.from_arrays(list_arrays, names))

    @classmethod
    def from_pandas(cls, df: pd.DataFrame) -> "ArrowTable":
        """Create an ArrowTable from a pandas DataFrame"""
        return cls(pa.table(df))

    def to_pandas(self, *, self_destruct: bool = False) -> pd.DataFrame:
        """
        Converts the table to a pandas dataframe. Might lead to memory doubling for some types, so
        set the second argument for cost-cutting, but then you can't access the original table again.
        """
        return self._data.to_pandas(
            split_blocks=True,
            self_destruct=self_destruct,
            types_mapper=_arrow_to_pandas_type,
        )

    def column(self, col: Union[str, int]) -> np.ndarray:
        """read the table column as a numpy array"""
        return to_numpy_array(self._data.column(col))

    def select(self, cols: List[str]) -> "ArrowTable":
        """select a subset of the columns in the table"""
        assert all(
            s in self._data.column_names for s in cols
        ), "All input columns are not present in the table, check with the schema"
        return type(self)(self._data.select(cols))

    def filter(self, mask: np.ndarray) -> "ArrowTable":
        """
        Filter the table given a boolean mask array. The Arrow compute library is very prelim, so only
        add the very common routines. Convert to pandas otherwise.

        NOTE<dev>: Ugh, looks like Arrow kernels materialize the data in memory so we could just work in
        pandas land directly.
        """
        return type(self)(self._data.filter(pa.array(mask)))

    @property
    def schema(self) -> pa.Schema:
        return self._data.schema

    @property
    def data(self) -> pa.Table:
        return self._data

    @property
    def num_rows(self) -> int:
        return self._data.num_rows

    @property
    def num_columns(self) -> int:
        return self._data.num_columns

    @property
    def dtypes(self) -> Dict[str, str]:
        return {f.name: str(f.type) for f in self._data.schema}


class ArrowWriter:
    """
    Write to an Arrow IPC file - an entire pandas dataframe/pyarrow table
    at once, or one record batch at a time.
    """

    _stream: Union[pa.CompressedOutputStream, pa.OSFile]
    _writer: pa.RecordBatchFileWriter
    schema: pa.Schema

    def __init__(self, filename: str, schema: pa.Schema) -> None:
        """
        File name with a `.zst` extension will be zstd compressed. Also, to get
        the schema from a pandas dataframe, you can do `pa.Schema.from_pandas(df)`.
        """
        self.schema = schema
        self._stream = pa.OSFile(filename, "wb")
        if filename.endswith(".zst"):
            self._stream = pa.CompressedOutputStream(self._stream, "zstd")
        self._writer = pa.ipc.new_file(self._stream, schema)

    def close(self) -> None:
        """MUST call this after done with writing record batches to the file"""
        self._writer.close()
        self._stream.close()

    def __enter__(self):
        """to use as a context manager"""
        return self

    def __exit__(self, *exc_args):
        """to use as a context manager"""
        self.close()

    def _write_internal(self, batch: Union[pa.RecordBatch, pa.Table]):
        """Write a record batch or table to the file stream"""
        assert (
            self.schema == batch.schema
        ), "Schema of record-batch/table doesn't match the description"
        if isinstance(batch, pa.RecordBatch):
            self._writer.write_batch(batch)
        else:
            self._writer.write_table(batch)

    def write_table(self, table: ArrowTable):
        """
        Write a table or a chunk to the file stream.

        NOTE: Always call from inside the context manager
        """
        assert isinstance(
            table, ArrowTable
        ), f"Expected `table` of type `ArrowTable`, received {type(table)}"
        self._write_internal(table.data)


class ArrowReader:
    """
    For a compressed input file, this reads it as a RecordBatch stream, so
    it can be read in chunks or loaded in memory all at once.
    With an uncompressed file, this wrapper returns a memory mapped table,
    """

    _invalidated: bool = False
    _compressed: bool
    _stream: Union[pa.MemoryMappedFile, pa.CompressedInputStream]
    _reader: Union[pa.RecordBatchFileReader, pa.RecordBatchStreamReader]
    schema: pa.Schema

    def __init__(self, filename: str) -> None:
        self._compressed = True if filename.endswith(".zst") else False
        if self._compressed:
            self._stream = pa.CompressedInputStream(pa.OSFile(filename, "r"), "zstd")
            self._stream.read(8)  # read past the Arrow magic string
            self._reader = pa.ipc.open_stream(self._stream)
        else:
            self._stream = pa.memory_map(filename, "r")
            self._reader = pa.ipc.open_file(self._stream)
        self.schema = self._reader.schema

    def _check_invalidated(self, set_val: bool) -> bool:
        if self._invalidated:
            raise Exception(
                "The current reader stream is exhausted, create a new reader instance"
            )
        self._invalidated = set_val

    def read_table(self, *, _internal_type: bool = False) -> "ArrowTable":
        """
        Provide the entire Arrow table at once. With a compressed input file, this
        will materialize the entire table in memory.
        """
        self._check_invalidated(True)
        table = self._reader.read_all()
        return table if _internal_type else ArrowTable(table)

    def read_batch_stream(
        self, *, _internal_type: bool = False
    ) -> Iterator["ArrowTable"]:
        """Provides a generator function that yields the table, one chunk at a time"""
        self._check_invalidated(True)
        if isinstance(self._reader, pa.RecordBatchFileReader):
            for idx in range(self._reader.num_record_batches):
                batch = self._reader.get_record_batch(idx)
                yield batch if _internal_type else ArrowTable(batch)
        elif isinstance(self._reader, pa.RecordBatchStreamReader):
            for batch in self._reader:
                yield batch if _internal_type else ArrowTable(batch)


def save_dataframe(fname, df):
    """convenience function to save a pandas dataframe"""
    with ArrowWriter(fname, pa.Schema.from_pandas(df)) as writer:
        writer.write_table(ArrowTable.from_pandas(df))


def read_dataframe(fname):
    """convenience function to read a pandas dataframe saved as an arrow file"""
    return ArrowReader(fname).read_table().to_pandas()
