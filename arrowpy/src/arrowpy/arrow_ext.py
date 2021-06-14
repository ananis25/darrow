"""
This module implements an Arrow extension type for numpy multidim arrays, with the 
underlying storage type as binary. 
"""

import json
import logging
from typing import Tuple, Optional, Union

import numpy as np
import pyarrow as pa

logger = logging.getLogger(__name__)


def _arrow_to_numpy_array(
    arrow_arr: Union[pa.Array, pa.ExtensionArray], 
    sub_shape: Optional[Tuple[int, ...]] = None, 
    dtype: Optional[np.dtype] = None
) -> np.ndarray:
    """
    Convert an Arrow array to a numpy array. The dtype and shape arguments are necessary 
    only for deserializing binary array inputs, so the output is a numpy array of the 
    given type and shape - `(len(input), *sub_shape)`. 
    """
    if not isinstance(arrow_arr, pa.ExtensionArray):
        return arrow_arr.to_numpy(zero_copy_only=False)
    assert pa.types.is_large_binary(arrow_arr.storage.type), f"Unexpected input array type: {arrow_arr.storage.type}"
    assert sub_shape is not None, "Expect a non null sub_shape input for extension array"
    assert dtype is not None, "Expect a non null dtype input for extension array"

    elem_size = dtype.itemsize * np.prod(sub_shape)
    out_shape = (len(arrow_arr), *sub_shape)
    no_nulls, offsets_buffer, data_buffer = arrow_arr.storage.buffers()
    offsets = np.frombuffer(offsets_buffer, dtype=np.int64)

    assert (offsets[1] - offsets[0]) == elem_size, "Input array buffers don't match the expected shape"
    assert data_buffer.size == elem_size * len(arrow_arr), "Input array buffers don't match the expected shape"
    return np.frombuffer(data_buffer, dtype=dtype).reshape(out_shape)


def _numpy_to_arrow_array(array: np.ndarray) -> pa.Array:
    """
    Convert a numpy array to an Arrow array. If the array is multidim, we convert it to 
    the Arrow large binary array, treating it as an array of subarrays (serialized as bytes). 
    
    * NOTE<dev>: The large binary type is not supported everywhere but it can accomodate arrays 
    greater than 2 GB in size, so this is what we will use. 
    """
    num_elems = array.shape[0]  # Do we need a code path for one-dim numpy arrays? That lets unwanted inputs creep in. 
    data_buffer = pa.py_buffer(np.ascontiguousarray(array))
    offsets = pa.py_buffer(
        np.linspace(0, data_buffer.size, num=num_elems+1, dtype=np.int64)
    )
    return pa.Array.from_buffers(
        pa.large_binary(), num_elems, [None, offsets, data_buffer]
    )


class ArrowNumpyExtType(pa.ExtensionType):
    """Arrow extension type for tensor like arrays"""
    _sub_shape: Tuple[int, ...]
    _dtype: np.dtype

    def __init__(self, sub_shape: Tuple[int, ...], ndtype: np.dtype):
        assert isinstance(ndtype, np.dtype), f"Expected `ndtype` of type `np.dtype`, received {type(ndtype)}"
        self._sub_shape = tuple(sub_shape)
        self._dtype = ndtype
        pa.ExtensionType.__init__(self, pa.large_binary(), 'numpy.ext')

    @property
    def sub_shape(self):
        return self._sub_shape
    
    @property
    def ndtype(self):
        return self._dtype
    
    def __arrow_ext_serialize__(self) -> bytes:
        return json.dumps({"ndtype": str(self.ndtype), "sub_shape": self.sub_shape}).encode()
    
    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized: str) -> 'ArrowNumpyExtType':
        props = json.loads(serialized)
        return cls(props['sub_shape'], np.dtype(props['ndtype']))
    
    def __arrow_ext_class__(self):
        return ArrowNumpyExtArray

    def to_pandas_dtype(self):
        from .pandas_ext import NumpyExtType
        return NumpyExtType(self.sub_shape, self.ndtype)


class ArrowNumpyExtArray(pa.ExtensionArray):
    """
    Arrow extension array to store tensors. 
    """
    # type: ArrowNumpyExtType

    def to_numpy(self):
        return _arrow_to_numpy_array(self, self.type.sub_shape, self.type.ndtype)
    
    @classmethod
    def from_numpy(cls, array: np.ndarray):
        storage = _numpy_to_arrow_array(array)
        ext_type = ArrowNumpyExtType(array.shape[1:], array.dtype)
        return pa.ExtensionArray.from_storage(ext_type, storage)


def to_numpy_array(array: Union[pa.Array, pa.ChunkedArray]):
    """
    NOTE<dev>: This method is needed since the `to_numpy` method on the array object 
    doesn't work if the extension array is boxed in a chunked array. 
    """
    if isinstance(array, pa.ChunkedArray):
        if isinstance(array.type, ArrowNumpyExtType):
            return np.vstack([chunk.to_numpy() for chunk in array.chunks])
    return array.to_numpy()


# register the extension types for Arrow 
_registered_types = [typ.extension_name for typ in pa.types.lib._python_extension_types_registry]
# check needed for hot reloading in tools like streamlit
if 'numpy.ext' not in _registered_types:
    pa.register_extension_type(ArrowNumpyExtType((2,1), np.dtype('int16')))

__all__ = ["ArrowNumpyExtType", "ArrowNumpyExtArray", "to_numpy_array"]