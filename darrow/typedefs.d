/** Collect imports for different datatypes across the Arrow Glib bindings in a 
single module. Also lets us limit the types to expose to Blackbird users. 

The Arrow API uses raw pointers throughout. Most of the classes in this module exist 
only to make sure raw pointer access is minimized and I can track their lifetimes. 

* NOTE<dev>: The library has some unpredicatable quirks. For ex - when reading an arrow 
file, even if one of the columns is of type string, the record batch memory doesn't get 
released until closing off the stream. 
*/

module darrow.typedefs;

import std.conv: to;
import std.stdio;
import std.traits;
import std.exception;
import std.file: exists;
import std.format: format;
import std.string: toStringz, fromStringz;
import std.uni: asLowerCase;
import std.utf: toUTFz;
import std.meta: Alias;
import std.range.primitives: ElementType;

import darrow.arrow_bindings;


enum ulong PLACEHOLDER_BATCH_SIZE = 128_000;  // TOOD: what should be a reasonable number?

/** get the array type for a scalar type */
template typeScalarToArray(T)  { alias typeScalarToArray = const(T)[]; }
template typeScalarToArrowArray(T)  { alias typeScalarToArrowArray = ArrowArray!T; }
template BaseType(T)
{
	static if (isStaticArray!T) { alias BaseType = BaseType!(ElementType!T); }
    else { alias BaseType = T; }
}
ulong[] typeToShape(T)()
{
    static if (isStaticArray!T) { return [ T.length ] ~ typeToShape!(ElementType!T)(); }
    else { return []; }
}


/** raise a Glib exception */
void checkGlibError(GError* err)
{
    if (err !is null)
        throw new Exception( format("Error code: %s, msg: %s", err.code, fromStringz(err.message)) );
}


/** Return the Arrow equivalent type as a string for a Dlang type */
string typeFromDlangToArrow(T)()
{
    static if (isStaticArray!T) return "LargeBinary";

    else static if (is(T==byte)) return "Int8";
    else static if (is(T==short)) return "Int16";
    else static if (is(T==int)) return "Int32";
    else static if (is(T==long)) return "Int64";
    
    else static if (is(T==ubyte)) return "UInt8";
    else static if (is(T==ushort)) return "UInt16";
    else static if (is(T==uint)) return "UInt32";
    else static if (is(T==ulong)) return "UInt64";
    
    else static if (is(T==float)) return "Float";
    else static if (is(T==double)) return "Double";
    
    else static if (is(T==bool)) return "Boolean";
    
    else static if (is(T==string)) return "String";
    // TODO: We should support only a single timestamp type. Look up which is the most useful.
    else 
        static assert(false, "No Arrow datatype implemented for field type: " ~ T.stringof);
}

/** Return the numpy equivalent type for a Dlang type since numpy doesn't parse 
them the same way as This is for multidim arrays, so we only handle numerics. 
*/
enum string[string] TypeMapDlangToNumpy = [
    "byte": "int8", 
    "short": "int16", 
    "int": "int32", 
    "long": "int64", 
    "ubyte": "uint8", 
    "ushort": "uint16", 
    "uint": "uint32", 
    "ulong": "uint64", 
    "float": "float32",
    "double": "float64",
    "bool": "bool",
];


/** abstraction around a linked list used by Arrow functions */
final class ArrowList
{
    private GList* gList;

    GList* getStruct() { return gList; }

    /** constructor */
    this(GList* gList_) { this.gList = gList_; }

    // ~this() { this.release(); }

    void release() { g_list_free_full(this.gList, &g_object_unref); }

    /** append a new element to the linked list */
    bool append(void* data)
    {
        this.gList = g_list_append(gList, data);
        return (this.gList is null) ? false : true;
    }
}


/** wrapper around the GArrowDataType* data structure */
class BaseDataType
{
    private GArrowDataType* gArrowDataType;

    /** constructor */
    this(GArrowDataType* gArrowDataType_) { this.gArrowDataType = gArrowDataType_; }

    /** return the Arrow datatype struct */
    GArrowDataType* getBaseStruct() { return this.gArrowDataType; }
}


/** wrapper around the GArrowField* data structure */
final class ArrowField
{
    private GArrowField* gArrowField;
    private BaseDataType dataType;

    GArrowField* getStruct() { return this.gArrowField; }

    BaseDataType getDataType() { return this.dataType; }

    /** constructor */
    this(string name, BaseDataType dataType_, GHashTable* metadata = null)
    {
        this.dataType = dataType_;
        // the last argument indicates if nullable fields are allowed
        gArrowField = garrow_field_new_full(toStringz(name), dataType_.getBaseStruct(), 1);
        if (metadata !is null)
            gArrowField = garrow_field_with_metadata(gArrowField, metadata);
    }
}


/** wrapper around the GArrowSchema* data structure */
final class Schema
{
    private GArrowSchema* gArrowSchema;

    GArrowSchema* getStruct() { return gArrowSchema; }

    /** constructor */
    this(ArrowField[] fields, GHashTable* metadata)
    {
        import std.system: endian, Endian;
        assert (endian == Endian.littleEndian, 
            "Raise a PR, compatibility with big endian systems is unassessed. Why do you have one anyway?");

        auto listFields = new ArrowList(null);
        foreach (field; fields) {
            auto status = listFields.append( field.getStruct() );
            enforce (status==true, "could not append new field to the Arrow list");
        }

        auto _schema = garrow_schema_new( listFields.getStruct() );
        this.gArrowSchema = garrow_schema_with_metadata(_schema, metadata);
    }

    /** alternate constructor */
    this(GArrowSchema* schema_) { this.gArrowSchema = schema_; }

    override string toString()
    {
        return assumeUnique( fromStringz(garrow_schema_to_string(this.gArrowSchema)) );
    }

    /** check if two schemas are equal */
    bool checkEqual(Schema otherSchema)
    {
        return garrow_schema_equal(this.getStruct, otherSchema.getStruct) != 0;
    }

    /** Get the Arrow table schema from the struct definition */
    static Schema makeFromStruct(T)()
    {
        import std.format: format;
        import std.json: JSONValue;

        ArrowField[] fieldList;
        GHashTable* metadataSchema = g_hash_table_new(&g_str_hash, &g_str_equal);  // for columns of the type static array

        alias memberTypes = Fields!T;
        // why does the stdlib discriminate between tuples and lists! Just let me use the same itertools everywhere. 
        foreach (idx, name; FieldNameTuple!T) 
        { 
            alias dType = memberTypes[idx];
            auto fieldDataType = new DataType!dType();
            if (isStaticArray!dType)
            {
                string ElemType = BaseType!dType.stringof;
                assert (ElemType != "bool", 
                    "Arrow likes 8 byte boundaries and uses int64 for booleans, I don't want to think about it.");
                auto jj = JSONValue(string[string].init);
                jj.object["sub_shape"] = typeToShape!dType;
                jj.object["ndtype"] = TypeMapDlangToNumpy[ ElemType ];

                int added;
                GHashTable* metadata = g_hash_table_new(&g_str_hash, &g_str_equal);
                added = g_hash_table_insert(metadata, 
                    toUTFz!(char*)("ARROW:extension:name"), toUTFz!(char*)("numpy.ext"));
                assert (added != 0, "Hashtable storing field metadata was not created correctly");
                added = g_hash_table_insert(metadata, 
                    toUTFz!(char*)("ARROW:extension:metadata"), toUTFz!(char*)(jj.toString()));
                assert (added != 0, "Hashtable storing field metadata was not created correctly");
                fieldList ~= new ArrowField(name, fieldDataType, metadata);
            }
            else
            {
                fieldList ~= new ArrowField(name, fieldDataType);
            }
        }
        return new Schema(fieldList, metadataSchema);
    }    
}


/** wrapper around the type specialized GArrowDataType* data structure */
class DataType(DType): BaseDataType
{    
    enum string TypeAsStr = typeFromDlangToArrow!DType;
    static if (isStaticArray!DType) 
    {
        alias ProxyType = GArrowLargeBinaryDataType;
        alias makeProxyType = garrow_large_binary_data_type_new;
    } else {
        alias ProxyType = Alias!( mixin(format("GArrow%sDataType", TypeAsStr)) );
        alias makeProxyType = Alias!( mixin(format("garrow_%s_data_type_new", TypeAsStr.asLowerCase)) );
    }

    private ProxyType* gArrowSpecDataType;  // Arrow type-specialized data type struct

	ProxyType* getStruct() { return gArrowSpecDataType; }
    
    /** constructor */
    this()
    {
        auto _type = makeProxyType();
        if (_type is null) 
            throw new Exception( format("type constructor for %s returned null", _type) );
        this.gArrowSpecDataType = _type;
        super(cast(GArrowDataType*)_type);
    }
}

// NOTE<dev>: Relevant code to handle List data types was present in earlier version of this 
// repository before we moved to using binary datatype for static arrays.


/** wrapper around the GArrowArrayBuilder* data structure */
class BaseArrayBuilder
{
    private GArrowArrayBuilder* gArrowArrayBuilder;
    private GError* err = null;

    /** constructor */
    this(GArrowArrayBuilder* gArrowArrayBuilder_) { this.gArrowArrayBuilder = gArrowArrayBuilder_; }

    GArrowArrayBuilder* getBaseStruct() { return this.gArrowArrayBuilder; }

    GArrowArray* finish()
    {
		GArrowArray* array_ptr = garrow_array_builder_finish(gArrowArrayBuilder, &err);
        checkGlibError(err);
        return array_ptr;
    }

    void release() { g_object_unref(this.gArrowArrayBuilder); }    
}


/** wrapper around the type specialized GArrowArrayBuilder* data structure */
class ArrayBuilder(DType): BaseArrayBuilder
{
    enum string TypeAsStr = typeFromDlangToArrow!DType;
    static if (isStaticArray!DType) 
    {
        alias ProxyBuilderType = GArrowLargeBinaryArrayBuilder;
        alias makeProxyBuilder = garrow_large_binary_array_builder_new;
    } else {
        alias ProxyBuilderType = Alias!( mixin(format("GArrow%sArrayBuilder", TypeAsStr)) );
        alias makeProxyBuilder = Alias!( mixin( format("garrow_%s_array_builder_new", TypeAsStr.asLowerCase)) );
    }

    private ProxyBuilderType* gArrowSpecArrayBuilder;

    ProxyBuilderType* getStruct() { return gArrowSpecArrayBuilder; }

    /** constructor */
    this()
    {
        this.gArrowSpecArrayBuilder = makeProxyBuilder();
        super(cast(GArrowArrayBuilder*)gArrowSpecArrayBuilder);
    }

    /** alternative constructor */
    this(GArrowArrayBuilder* gArrowArrayBuilder_)
    {
        this.gArrowSpecArrayBuilder = cast(ProxyBuilderType*)(gArrowArrayBuilder_);
        super(cast(GArrowArrayBuilder*)gArrowSpecArrayBuilder);
    }

    /** append values to the array builder */
    bool appendValues(DType[] data, int[] noNullFlags, long numValues)
    {
        static if (isStaticArray!DType) 
        {
            alias appender = garrow_large_binary_array_builder_append_values;
            auto typedData = new GBytes*[numValues];
            for (long i = 0; i < numValues; i++)
                typedData[i] = g_bytes_new(cast(ubyte*)(data[i].ptr), DType.sizeof);
            scope(exit) { for (long i = 0; i < numValues; i++) { g_bytes_unref(typedData[i]); } }
        } 
        else static if (is(DType==string)) 
        {
            alias appender = garrow_string_array_builder_append_strings;
            auto typedData = new const(char)*[numValues];
            for (long i = 0; i < numValues; i++) 
                typedData[i] = cast(char*)(data[i]~'\0');  // toStringz(data[i]);            
        } 
        else static if (is(DType==bool)) 
        {
            alias appender = garrow_boolean_array_builder_append_values;
            auto typedData = new int[numValues];
            for (long i = 0; i < numValues; i++) 
                typedData[i] = data[i] ? 1 : 0;
        }
        else
        {
            alias appender = Alias!( mixin(format("garrow_%s_array_builder_append_values", TypeAsStr.asLowerCase)) );
            auto typedData = data;
        }

        auto status = appender(gArrowSpecArrayBuilder, typedData.ptr, numValues, noNullFlags.ptr, numValues, &err) != 0;
        checkGlibError(err);
        return status;
    }
}


final class ArrowArray(DType)
{
    enum string arrowType = typeFromDlangToArrow!DType();
    alias ArrayType = Alias!( mixin(format("GArrow%sArray", arrowType)) );

    private GArrowArray* baseStruct;
    private long numRows;
    static if (is(DType==bool) || is(DType==string)) {
        private DType[] dArray;
    } else {
        private const(DType)[] dArray;
    }
    static if (isStaticArray!DType) {
        private GArrowBuffer* _buffer;
        private GBytes* _gbytes;
    }

    /** constructor */
    this(GArrowArray* arrayStruct) { this.reset(arrayStruct); }

    ~this() { this.release(); }

    void release() 
    { 
        if (this.baseStruct !is null) {
            static if (isStaticArray!DType) {
                // g_bytes_unref(_gbytes);
                // g_object_unref(_buffer);
            }
            g_object_unref(this.baseStruct); 
            this.baseStruct = null;
        }
    }

    /** Arrow doesn't expose a D slice compatible array for the string/bool
    datatypes, so a dynamic array needs to be filled in with the raw values. This 
    method lets us recycle the same `ArrowArray` variable for multiple arrow arrays. 
    */
    void reset(GArrowArray* arrayStruct) 
    {
        this.release();
        this.baseStruct = arrayStruct;
        this.numRows = garrow_array_get_length(arrayStruct);
        static if (is(DType==string) || is(DType==bool))
        {
            if (this.dArray is null)
                this.dArray = new DType[this.numRows];
            else
                this.dArray.length = this.numRows;
        }

        this.setupData();
    }

    /** returns the array slice */
    const(DType)[] getData() { return this.dArray; }

    /** Fill the D slice with the arrow array data */
    void setupData()
    {
        static if (isStaticArray!DType)
        {
            ulong bufferLen;
            auto binaryArrowArray = cast(GArrowLargeBinaryArray*)baseStruct;
            auto _buffer = garrow_large_binary_array_get_data_buffer(binaryArrowArray); 
            auto _gbytes = garrow_buffer_get_data(_buffer);
            auto bytes = g_bytes_get_data(_gbytes, &bufferLen);
            auto data = cast(ubyte[])(bytes[0 .. bufferLen]);
            
            const elemLen = DType.sizeof;
            assert (this.numRows*elemLen == bufferLen, "Input binary array doesn't match the size expected");
            this.dArray = cast(const(DType)[])(data[0 .. bufferLen]);
        }
        else static if (is(DType==string))
        {
            auto stringArrowArray = cast(GArrowStringArray*)baseStruct;
            for (long i = 0; i < this.numRows; i++) {
                char* value = garrow_string_array_get_string(stringArrowArray, i);
                this.dArray[i] = value[0 .. strlen(value)].idup;
            }
        }
        else static if (is(DType==bool))
        {
            long length;
            auto rawValues = garrow_boolean_array_get_values(cast(ArrayType*)baseStruct, &length);
            for (int i=0; i<length; i++) {
                this.dArray[i] = rawValues[i] != 0;
            }
        }
        else 
        {
            long length;
            alias readFunction = Alias!( mixin(format("garrow_%s_array_get_values", arrowType.asLowerCase)) );
            auto rawValues = readFunction( cast(ArrayType*)baseStruct, &length );
            this.dArray = cast(const(DType)[])(rawValues[0 .. length]);
        }
    }
}  // many brackets, very wow


/** 
Abstraction representing an Arrow chunked array, accessible like a range from a `foreach` loop.

NOTE: This is not a standard range, raise a PR if you want to iterate over two or more of these together. 
*/
struct ArrowRange(DType)
{
    enum string arrowType = typeFromDlangToArrow!DType;
    alias ArrayType = Alias!( mixin(format("GArrow%sArray", arrowType)) );

    private GArrowChunkedArray* baseStruct;
    private ulong numRows;
    private uint numChunks = 0;
    private uint currentChunk;

    /** constructor */
    this(GArrowChunkedArray* chunkedStruct) 
    { 
        this.baseStruct = chunkedStruct; 
        this.numRows = garrow_chunked_array_get_n_rows(chunkedStruct);
        this.numChunks = garrow_chunked_array_get_n_chunks(chunkedStruct);
    }

    ~this() { this.release(); }

    void release() { g_object_unref(this.baseStruct); }

    ulong getNumRows() { return this.numRows; }

    /** 
    Apparently the range interface works this way too, micro benchmarking tells me this is like 50 pc faster 
    than the 3 method interface setup. 
    Credits: http://ddili.org/ders/d.en/foreach_opapply.html

    EDIT: this just works with the `foreach` construct, it isn't really a range. 
    */
    int opApply(int delegate(ref const(DType)) dg) 
    {
        int result = 0;
        for (uint chunkIdx = 0; chunkIdx < this.numChunks ; chunkIdx++) 
        {
            long length;
            auto baseArray = garrow_chunked_array_get_chunk(this.baseStruct, chunkIdx);

            static if (isStaticArray!DType)
            {
                ulong bufferLen;
                auto binaryArray = cast(GArrowLargeBinaryArray*)baseArray;
                auto _buffer = garrow_large_binary_array_get_data_buffer(binaryArray); 
                auto _gbytes = garrow_buffer_get_data(_buffer);
                auto bytes = g_bytes_get_data(_gbytes, &bufferLen);
                auto data = cast(ubyte[])(bytes[0 .. bufferLen]);
                
                auto elemLen = DType.sizeof;
                length = garrow_array_get_length(baseArray);
                assert (length*elemLen == bufferLen, "Input binary array doesn't match the size expected");
                for (long i = 0; i < length; i++) {
                    result = dg(*cast(DType*)(data[i*elemLen .. (i+1)*elemLen]));
                    if (result) break;
                }
                g_bytes_unref(_gbytes);
                g_object_unref(_buffer);
            }
            else
            {
                alias readFunction = Alias!( mixin(format("garrow_%s_array_get_values", arrowType.asLowerCase)) );
                auto rawValues = readFunction( cast(ArrayType*)baseArray, &length );
                const(DType[]) typedArray = rawValues[0 .. length];

                for (long i = 0; i < length; i++) {
                    static if (is(DType==bool))
                        result = dg(typedArray[i] != 0);
                    else
                        result = dg(typedArray[i]);
                    if (result) break;
                }
            }
            g_object_unref(baseArray);
            if (result) break;
        }
        return result;
    }

    // bool empty() { assert(false, "Not implemented. This is not a regular range."); return false; }
    // DType front() { assert(false, "Not implemented. This is not a regular range."); return DType.init; }
    // void popFront() { assert(false, "Not implemented. This is not a regular range."); }
}


/** wrapper class for an Arrow record batch */
final class RecordBatch
{
private:
    GArrowRecordBatch* batchStruct;
    Schema schema;
    GError* err;

public:
    GArrowRecordBatch* getStruct() { return this.batchStruct; }

    /** constructor */
    this(GArrowRecordBatch* batch)
    {
        this.batchStruct = batch; 
        this.schema = new Schema(garrow_record_batch_get_schema(this.batchStruct));
    }

    /** alternate constructor */
    this(Schema schema, uint numRows, ArrowList columns)
    {
        GArrowRecordBatch* _batch = garrow_record_batch_new(schema.getStruct(), numRows, columns.getStruct(), &err);
        checkGlibError(err);
        this.batchStruct = _batch;
        this.schema = schema;
    }

    /** release the Arrow record batch reference, along with the column arrays */
    void release() 
    {
        if (this.batchStruct !is null) 
            g_object_unref(this.batchStruct);
    }

    Schema getSchema() { return this.schema; }

    long getNumRows() { return garrow_record_batch_get_n_rows(this.batchStruct); }

    long getNumColumns() { return garrow_record_batch_get_n_columns(this.batchStruct); }

    GArrowArray* getColumnData(int idx)
    {
        return garrow_record_batch_get_column_data(this.batchStruct, idx);
    }
}


/** Create and keep reference to an Arrow file buffer to read from */
final class ArrowInputStream
{
    private GArrowInputStream* baseStruct;
    private GError* err;

    GArrowInputStream* getStruct() { return this.baseStruct; }
    
    /** constructor */
    this(string fileName, bool compressed)
    {
        assert (exists(fileName), format("Attempting to read absent file: %s", fileName));
        if (compressed) 
        {
            auto options = garrow_local_file_system_options_new();
            auto fileSystem = cast(GArrowFileSystem*)( garrow_local_file_system_new(options) );
            auto _stream = garrow_file_system_open_input_stream(fileSystem, toStringz(fileName), &err);
            checkGlibError(err);

            auto _codec = garrow_codec_new(GArrowCompressionType.GARROW_COMPRESSION_TYPE_ZSTD, &err);
            auto _comp_stream = garrow_compressed_input_stream_new(_codec, cast(GArrowInputStream*)_stream, &err);
            checkGlibError(err);
            this.baseStruct = cast(GArrowInputStream*)_comp_stream;
        } 
        else 
        {
            auto _stream = garrow_memory_mapped_input_stream_new(toStringz(fileName), &err);
            this.baseStruct = cast(GArrowInputStream*)_stream;
        } // use memory mapping with uncompressed files
    }

    /** close the input file stream */
    bool close() 
    {
        bool fileClosed = garrow_file_close(cast(GArrowFile*)(this.baseStruct), &err) != 0; 
        return fileClosed;
    }
}


/** Create and keep reference to an Arrow file buffer to write into */
final class ArrowOutputStream
{
    private GArrowOutputStream* baseStruct;
    private GError* err;

    GArrowOutputStream* getStruct() { return this.baseStruct; }

    /** constructor */
    this(string fileName, bool compress)
    {
        auto _stream = cast(GArrowOutputStream*)( garrow_file_output_stream_new(toStringz(fileName), false, &err) );
        checkGlibError(err);
        if (compress) 
        {
            auto _codec = garrow_codec_new(GArrowCompressionType.GARROW_COMPRESSION_TYPE_ZSTD, &err);
            checkGlibError(err);
            auto _comp_stream = garrow_compressed_output_stream_new(_codec, _stream, &err);
            checkGlibError(err);
            if (_comp_stream is null)
                throw new Exception("Arrow Zstd stream constructor returned null");
            this.baseStruct = cast(GArrowOutputStream*)(_comp_stream);
        } 
        else
            this.baseStruct = _stream;
    }

    /** close the output file stream */
    bool close() 
    {
        // UNEXPLAINED: The file close operation takes ~20s for uncompressed files, no clue why. 
        bool fileClosed = garrow_file_close(cast(GArrowFile*)(this.baseStruct), &err) != 0; 
        return fileClosed;
        // This way is slow too, so apparently a memory alloc thing? `g_object_unref(this.baseStruct); return true;``
    }    
}


/** wrapper class to read record batches one at a time from file */
final class RecordBatchStreamReader
{
private:
    ArrowInputStream inputStream;
    GArrowRecordBatchReader* readerStruct;
    string fileName;
    bool compress;

    GError* err = null;
    uint numBatchesRead;
    uint numBatchesTotal;     
    bool streamClosed;

public:
    GArrowRecordBatchReader* getStruct() { return readerStruct; }

    /** constructor */
    this(string fileName_, bool compress_)
    {
        this.compress = compress_;
        this.fileName = compress_ ? (fileName_ ~ ".zst") : fileName_;
        this.inputStream = new ArrowInputStream(this.fileName, this.compress);
        this.streamClosed = false;

        auto streamStruct = this.inputStream.getStruct();
        garrow_input_stream_advance(streamStruct, cast(long)8, &err);
        checkGlibError(err);

        this.readerStruct = cast(GArrowRecordBatchReader*)(garrow_record_batch_stream_reader_new(streamStruct, &err));
        checkGlibError(err);
    }

    // TODO: resolve the GC allocation error that happens with the destructor
    ~this() { this.close(); }

    /** close the input file stream */
    bool close() 
    { 
        if (!this.streamClosed)
            this.streamClosed = this.inputStream.close(); 
        return this.streamClosed;
    }

    /** read the next available record batch from the stream, returns null if none left */
    RecordBatch readBatch()
    {
        GArrowRecordBatch* batch = garrow_record_batch_reader_read_next(readerStruct, &err);
        checkGlibError(err);
        if (batch is null)
            return null;
        else
            return new RecordBatch(batch);
    }    
}


/** wrapper class to write down record batches to a file */
final class RecordBatchStreamWriter
{
private:
    ArrowOutputStream outputStream;
    GArrowRecordBatchWriter* writerStruct;
    Schema schema;
    string fileName;
    bool compress;
    GError* err = null;
    bool streamClosed;    

public:
    GArrowRecordBatchWriter* getStruct() { return writerStruct; }

    /** constructor */
    this(string fileName_, bool compress_, Schema schema)
    {
        this.schema = schema;
        this.compress = compress_;
        this.fileName = compress_ ? (fileName_ ~ ".zst") : fileName_;

        this.outputStream = new ArrowOutputStream(this.fileName, this.compress);
        this.writerStruct = cast(GArrowRecordBatchWriter*)(
            garrow_record_batch_file_writer_new(this.outputStream.getStruct, schema.getStruct(), &err)
        );
        checkGlibError(err);
    }

    // TODO: resolve the GC allocation error that happens with destructor
    ~this() { this.close(); }

    /** Close the output file writer, and stream. MUST call this after writing 
    all the data, else the last few batches don't get flushed to disk. 
    */
    bool close() 
    { 
        if (!this.streamClosed) {
            bool writerClosed = garrow_record_batch_writer_close(this.writerStruct, &err) != 0;
            bool fileClosed = this.outputStream.close();
            this.streamClosed = writerClosed && fileClosed;
        }
        return this.streamClosed;
    }    

    /** write down the given record batch to the file */
    bool writeBatch(RecordBatch batch)
    {
        auto status = garrow_record_batch_writer_write_record_batch(this.writerStruct, batch.getStruct, &err);
        checkGlibError(err);
        return status != 0;
    }
}