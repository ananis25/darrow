/** This module provides helpers to write tabular data to the Arrow 
format. The table abstraction here is only meant to dump/read data and 
not intended to be a dataframe. 
*/

module darrow.table;

import std.algorithm.iteration;
import std.meta;
import std.format: format;
import std.range: array;
import std.range.primitives: ElementType;
import std.traits;
import std.typecons: Tuple;
import std.stdio;

import darrow.arrow_bindings;
import darrow.typedefs;


/** An abstraction to streaming read the arrow IPC file format, one row at a time. 
*/
final class ArrowStreamReader(TypeStruct) {
private:
    RecordBatchStreamReader batchReader;
    Schema expectedSchema; 
    Tuple!(staticMap!(typeScalarToArrowArray, Fields!TypeStruct)) arrowSet;
    Tuple!(staticMap!(typeScalarToArray, Fields!TypeStruct)) columnSet;

    ulong numRowsRemaining;  // num of rows available in the current batch
    ulong numRowsTotal; // total num of rows in the current batch

public:
    bool empty = false;  // Range interface: check if the table has been exhausted
    
    /** constructor */
    this(string fileName_, bool compressedZstd_)
    {
        this.expectedSchema = Schema.makeFromStruct!TypeStruct();  // set down the schema of the Arrow table 
        this.batchReader = new RecordBatchStreamReader(fileName_, compressedZstd_);
        this.loadNextBatch(true);
    }

    /** Load next record batch to read from */
    void loadNextBatch(bool initialization = false)
    {
        auto batch = this.batchReader.readBatch();
        if (batch is null) {
            this.empty = true;
            return;
        }
        this.numRowsTotal = cast(ulong)(batch.getNumRows);
        this.numRowsRemaining = this.numRowsTotal;

        Schema batchSchema = batch.getSchema();
        assert (expectedSchema.checkEqual(batchSchema), 
            format("Schema expected:\n %s, received:\n %s", expectedSchema, batchSchema));

        // fill in the column values
        static foreach(idx, dType; Fields!TypeStruct) 
        {
            if (initialization)
                arrowSet[idx] = new ArrowArray!dType( batch.getColumnData(idx) );
            else 
                arrowSet[idx].reset( batch.getColumnData(idx) );
            columnSet[idx] = arrowSet[idx].getData();
        }
        batch.release();
    }

    /** Range interface: fetch the next row from the table */
    TypeStruct front()
    {
        TypeStruct data;
        foreach(idx, col; this.columnSet)
            data.tupleof[idx] = col[numRowsTotal - numRowsRemaining];
        return data;
    }

    /** Range interface: make an update after each row iteration */
    void popFront() 
    { 
        this.numRowsRemaining -= 1;
        if (this.numRowsRemaining==0)
            this.loadNextBatch();
    }
}


/** An abstraction to write the Arrow IPC format file, building it 
row by row. Doesn't support directly writing columns since Arrow needs 
them chunked in batches. 
*/
final class ArrowStreamWriter(TypeStruct) {
private:
    RecordBatchStreamWriter batchWriter;
    Schema expectedSchema; 
    enum ulong numCols = (Fields!TypeStruct).length;

    enum ulong batchLength = PLACEHOLDER_BATCH_SIZE;
    ulong currentRowCount;  // num of rows filled in the current batch
    TypeStruct[batchLength] batchData;

public:
    /** constructor */
    this(string fileName_, bool compressZstd_ = false)
    {
        this.expectedSchema = Schema.makeFromStruct!TypeStruct();  // set down the schema of the Arrow table 
        this.batchWriter = new RecordBatchStreamWriter(fileName_, compressZstd_, expectedSchema);
        this.batchData = new TypeStruct[batchLength];
        this.startNextBatch();
    }

    /** Finalize the data written until now. You only need to call it at the end 
    of your writing loop. 
    */
    bool finish()
    {
        this.finalizeBatchAndLoadNext();
        return this.batchWriter.close();
    }

    /** Initialize storage for next batch of data */
    private void startNextBatch()
    {
        // reset the batch buffer
        this.batchData[] = TypeStruct.init;
        currentRowCount = 0;
    }

    /** Flush the current batched data to the file and create a new record batch. */
    private void finalizeBatchAndLoadNext()
    {
        assert (currentRowCount <= batchLength, "ERROR: row index should never exceed specified batch length");
        if (currentRowCount == 0) 
            return;

        auto listColumns = new ArrowList(null);

        alias memberTypes = Fields!TypeStruct;
        foreach(idx, colName; FieldNameTuple!TypeStruct) 
        {
            alias dType = memberTypes[idx];
            auto filledBatch = this.batchData[0 .. currentRowCount];
            auto noNullFlags = new int[currentRowCount];
            noNullFlags[] = 1;  // TODO: allow null values at runtime cost
            
            auto colBuilder = new ArrayBuilder!dType();
            colBuilder.appendValues(filledBatch.map!(x => x.tupleof[idx]).array, noNullFlags, currentRowCount);
            listColumns.append( colBuilder.finish() );
            colBuilder.release();
        }

        // write the record batch to the file
        auto batch = new RecordBatch(this.expectedSchema, cast(uint)currentRowCount, listColumns);
        this.batchWriter.writeBatch(batch);
        listColumns.release();
        batch.release();
        this.startNextBatch();
    }

    /** Add a row to the Arrow file. 
    
    N.B. we keep the data for a record batch buffered in memory till it reaches 
    a certain size, and only then buffer it to a record batch. Since Arrow is a 
    columnar format and not a transactional store, you wouldn't want to create new 
    columns on every row write.
    */
    void writeRow(TypeStruct data)
    {
        import std.format: format;
        if (this.currentRowCount >= batchLength) {
            this.finalizeBatchAndLoadNext();
            assert (currentRowCount==0, format("current row count expected: 0, value: %d", currentRowCount));
        }
        this.batchData[currentRowCount] = data;
        currentRowCount++;
    }
}


/** Table abstraction to read an uncompressed arrow IPC file by memory 
mapping it, and iterating over columns one at a time as D ranges. 
Columns of string type are not supported, since that'd need allocation.
*/
final class ArrowColumnReader(TypeStruct)
{
private:
    ArrowInputStream inputStream;
    GArrowTable* tableStruct;
    Schema expectedSchema; 
    alias columnTypes = Fields!TypeStruct;
    alias columnNames = FieldNameTuple!TypeStruct;

    ulong numRows;

public:
    /** constructor */
    this(string fileName_)
    {
        assert (fileName_[$-4 .. $] != ".zst", "Only uncompressed files can be memory mapped");
        this.inputStream = new ArrowInputStream(fileName_, false);
        this.expectedSchema = Schema.makeFromStruct!TypeStruct();  // set down the schema of the Arrow table 

        GError* err = null;
        auto reader = garrow_feather_file_reader_new(
            cast(GArrowSeekableInputStream*)(this.inputStream.getStruct), &err);
        this.tableStruct = garrow_feather_file_reader_read(reader, &err);
        this.numRows = garrow_table_get_n_rows(this.tableStruct);

        Schema tableSchema = new Schema( garrow_table_get_schema(this.tableStruct) );
        assert (expectedSchema.checkEqual(tableSchema), 
            format("Schema expected:\n %s, received:\n %s", expectedSchema, tableSchema));
    }

    ~this()
    {
        g_object_unref(this.tableStruct);
        this.inputStream.close();
    }

    auto getColumn(string colName)()
    {
        alias idx = staticIndexOf!(colName, columnNames);
        static assert (idx >= 0, format("Column not found in the table: %s", colName));
        alias T = columnTypes[idx];
        static assert (!is(T==string), format("Column type: %s is unsupported", T.stringof));        

        return ArrowRange!T( garrow_table_get_column_data(this.tableStruct, idx) );
    }
}