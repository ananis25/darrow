/** This module provides helpers to write tensors to the Arrow 
binary format. 
*/

module darrow.tensors;

import std.algorithm.iteration;
import std.exception;
import std.meta: staticMap;
import std.format: format;
import std.range;
import std.range.primitives: ElementType;
import std.string: fromStringz;
import std.traits;
import std.typecons: Tuple;
import std.utf: toUTFz;

import darrow.arrow_bindings;
import darrow.typedefs;


/** Arrow data structure representing a multi-dim array. Try not 
to use this directly, and copy the data buffer to your favorite type.  
 */
struct ArrowTensor(DType)
{
    private GArrowTensor* baseStruct;
    private GBytes* byteBuffer;  // used only when reading from the tensor

    GArrowTensor* getStruct() { return this.baseStruct; }

    /** alternate constructor */
    private this(GArrowTensor* gArrowTensorStruct) 
    { 
        this.baseStruct = gArrowTensorStruct; 

        // check the tensor has the expected type
        auto _expTypeLocal = new DataType!DType();
        auto expType = _expTypeLocal.getBaseStruct();
        auto dataType = garrow_tensor_get_value_data_type(gArrowTensorStruct);
        assert (garrow_data_type_equal(expType, dataType) != 0, 
            format("Tensor provided has a different type: %s than expected: %s", 
                    garrow_data_type_get_id(dataType), DType.stringof));
    }

    /** Constructor. The data buffer is interpreted to be a contiguous 
    array laid out in the row major order. 
    */
    this(DType[] dataBuffer, long[] shape, string[] dimNames)
    {
        assert (shape.length == dimNames.length, "num of dimension doesn't equal the num of names");
        ulong numDims = cast(ulong)(shape.length);
        auto names = dimNames.map!(s => toUTFz!(char*)(s)).array;

        auto strides = new long[numDims]; 
        strides[$-1] = DType.sizeof;
        long size = shape[$-1];
        for (int i = cast(int)(numDims) - 1; i > 0; i--) {
            strides[i-1] = strides[i] * shape[i];
            size *= shape[i-1];
        }
        assert (dataBuffer.length == size, "size of data buffer doesn't match the shape");
        
        auto _dataType = new DataType!DType();
        auto _bytes = cast(ubyte[])( dataBuffer );
        auto _buffer = garrow_buffer_new(_bytes.ptr, size * DType.sizeof);
        GArrowTensor* _tensor = garrow_tensor_new(_dataType.getBaseStruct(), _buffer, 
            shape.ptr, numDims, strides.ptr, numDims, names.ptr, numDims);

        this.baseStruct = _tensor;
    }

    ~this() { this.release(); }

    /** release the Arrow allocated memory. */
    void release() 
    { 
        // TOOD: do I need to free the byte buffer separately? 
        if (this.byteBuffer !is null) {
            g_bytes_unref(this.byteBuffer);
            g_object_unref(this.baseStruct); 
        }
    }

    long[] getShape() 
    {
        int numDims;
        auto _shape = garrow_tensor_get_shape(this.baseStruct, &numDims);
        return _shape[0 .. numDims];
    }

    long[] getStrides() 
    {
        int numDims;
        auto _strides = garrow_tensor_get_strides(this.baseStruct, &numDims);
        return _strides[0 .. numDims];
    }

    /** total number of elements in the tensor */
    long getSize()
    {
        return garrow_tensor_get_size(this.baseStruct);
    }

    string[] getDimNames() 
    {
        int numDims = garrow_tensor_get_n_dimensions(this.baseStruct);
        auto names = new string[numDims];
        for (int i = 0; i < numDims; i++)
            names[i] = assumeUnique( fromStringz( garrow_tensor_get_dimension_name(this.baseStruct, i) ) );
        return names;
    }

    /** get the data buffer underlying the tensor as a contiguous array of the specified 
    type. Lifetime of this data is not clear to me, so copy to your preferred tensor type 
    instead of passing this around. */
    DType[] getDataBuffer()
    {
        ulong numBytes;
        this.byteBuffer = garrow_buffer_get_data( garrow_tensor_get_buffer(this.baseStruct) );
        auto _bytes = g_bytes_get_data(this.byteBuffer, &numBytes);

        ulong expBytes = DType.sizeof * garrow_tensor_get_size(this.baseStruct);
        assert (numBytes == expBytes, format("Tensor size, expected: %d, received: %d", expBytes, numBytes));
        return cast(DType[])( _bytes[0 .. numBytes] );
    }
}


/** Reading a tensor from a file */
final class ArrowTensorReader(DType)
{
    private ArrowInputStream stream;
    private GArrowTensor* tensorStruct;
    private GError* err = null;

    /** constructor */
    this(string fileName, bool compressZstd)
    {
        fileName = compressZstd ? (fileName ~ ".zst") : fileName;
        this.stream = new ArrowInputStream(fileName, compressZstd);
    }

    /** Read the tensor. If the input file is uncompressed, the file would  
    be memory mapped and no new memory should get allocated. 
    */
    ArrowTensor!DType read()
    {
        auto tensorStruct = garrow_input_stream_read_tensor(stream.getStruct, &err);
        if (err !is null)
            throw new Exception( format("Error code: %s, msg: %s", err.code, fromStringz(err.message)) );
        
        return ArrowTensor!DType(tensorStruct);
    }
}


/** Writing a tensor to a file */
final class ArrowTensorWriter(DType)
{
    private ArrowOutputStream stream;
    private GError* err = null;

    /** constructor */
    this(string fileName, bool compressZstd)
    {
        fileName = compressZstd ? (fileName ~ ".zst") : fileName;
        this.stream = new ArrowOutputStream(fileName, compressZstd);
    }

    /** Do the writing */
    void write(ArrowTensor!DType tensor)
    {
        auto numBytesWritten = garrow_output_stream_write_tensor(this.stream.getStruct, tensor.getStruct, &err);
        if (err !is null)
            throw new Exception( format("Error code: %s, msg: %s", err.code, fromStringz(err.message)) );

        long expBytes = tensor.getSize() * DType.sizeof;
        assert (numBytesWritten > expBytes, 
            format("Num of bytes written (%d) should exceed the data size (%d)", numBytesWritten, expBytes));
        this.stream.close();
    }
}


// Do not look here on, ugly template code.

/** flatten a nested D array into a contiguous array of the 
specified type. 
*/
BaseType!T[] flatten(T)(T input)
{
	static enum numDims = countDimensions!T;

    auto totalSize = countSize(input);    
    auto output = new BaseType!T[totalSize]; 

    int i = 0;
    mixin(format("auto arr%d = input;", numDims));
	mixin(loopy(numDims));
    return output;
}

private template BaseType(T)
{
	static if(isInputRange!T || isArray!T) alias BaseType = BaseType!(ElementType!T);
    else alias BaseType = T;
}

private ulong countDimensions(T)()
{
    static if (isArray!T) return 1 + countDimensions!(ElementType!T);
    else return 0;
}

private ulong countSize(T)(T array)
{
    static if (isArray!T) return array.length * countSize(array[0]);
    else return 1;
}

private string loopy(ulong n)
{
    if (n==0)
        return "output[i] = arr0; i = i + 1;";
    return format("foreach(arr%d; arr%d){ %s }", n-1, n, loopy(n-1));
}
