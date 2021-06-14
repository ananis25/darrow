Dlang library for users of the Apache Arrow data format, using bindings to the Glib-C API provided by the project. The `arrow_bindings.d` module is an amalgam of all the C routines and not intended to be used directly. The other modules provide helpers to read/write tables and tensors to the Arrow format. 

DEVELOPER NOTE: refer the C++ API documentation for implementation details, like why there is both an ArrowDataType and an ArrowType, described at [link](https://arrow.apache.org/docs/cpp/datatypes.html).
