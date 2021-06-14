"""
Multidim named arrow tensors. You don't need it. 
"""

from typing import List

import pyarrow as pa
import pyarrow.feather as feather
import pandas as pd
import numpy as np


def read_tensor(filename: str) -> pa.Tensor:
    """
    Read from an Arrow tensor file, using memory mapping if the file is 
    uncompressed. I haven't looked into how the memory mapping works with 
    edits, so create a copy if you're mutating the numpy array. 
    """
    compressed = True if filename.endswith('.zst') else False
    if compressed:
        return pa.ipc.read_tensor(pa.input_stream(filename, compression='zstd'))
    else:
        return pa.ipc.read_tensor(pa.memory_map(filename))


def write_tensor(filename: str, tensor: pa.Tensor) -> None:
    """Write a numpy array to an Arrow tensor file"""
    compressed = True if filename.endswith('.zst') else False
    out_stream = pa.output_stream(filename, compression=compressed)
    pa.ipc.write_tensor(tensor, out_stream)


def tensor_to_numpy(tensor: pa.Tensor) -> np.ndarray:
    """does what you'd expect"""
    return tensor.to_numpy()


def tensor_from_numpy(array: np.ndarray, dim_names: List[str]) -> pa.Tensor:
    """Does what you'd expect. Dimension names aren't really needed 
    but I'm not sure if D bindings allow that too.
    """
    return pa.Tensor.from_numpy(array, dim_names=dim_names)
