~Not usable~ ~Kind of works, check back later~
Works. See below if you need to use. 

## What is
Arrow is an in-memory, columnar format for binary data, think the layout of a pandas dataframe but without the compute methods alongside it. The advantage Arrow provides is client libraries in many languages, so you can transfer data between different runtimes. For ex - moving numpy like data buffers from a Dlang TCP socket to python. Increasingly, a lot of python packages like pandas can deserialize arrow data easily to their own format, or work directly on arrow data. 

For persistence, the Arrow libraries provide tools to read/write from csv/json/parquet. However, you can also write the Arrow _in-memory_ format directly to the disk which results in large files but avoids any (de)serialization costs and also unlocks memory mapping. 

The workflow I'm optimizing for is to write a large list of structs from D; and read it from python as columns. And vice versa. Writing to JSON from Dlang took 8 minutes for like 10 million rows, so to compensate I spent ~a few days~ around a week to do it with Arrow. 

The biggest win I see is out of core processing without loading all the data at once in memory - Pandas doesn't do it yet, but projects like [DuckDb](https://github.com/cwida/duckdb) do if you're comfortable enough with sql. Not that you can't bump up the RAM on your machine, I just don't like that aesthetic. Even if you keep using pandas, loading big dataframes should be a lot quicker compared to a csv, besides conserving the column datatypes.


## Why not use
* Mutation is expensive since Arrow also keeps a validity buffer along with the original data buffer. I haven't looked into how it plays out anyway. If you use dataframes that way, load the arrow file directly into a pandas dataframe and work there on. 
* No row level writes. Arrow is a _columnar_ format and stores data of the same type contiguously, albeit in chunks. When using the Dlang helper to write rows one at a time, they are buffered to the Arrow file only every _N_ rows. **Don't use it with live data**. 
* Segmentation faults: The Arrow project has well testted client libraries in python and other languages. For Dlang, we're using a C++ library with C wrappers called from D. Make your own conclusions. 
* Customizing will take work - you'd need to dig into C code, manually track and free variables (the Dlang GC doesn't handle memory for Arrow objects).


## Installation

### Arrow for Dlang (Currently supported version is: 2.0.0)

Refer to the `arrow-dev` folder, if you want to install from source (don't). Instead, use the system package manager since building from source is not fun.

Go to the [page](http://arrow.apache.org/install/) to install the binary dependencies for your platform. The dependencies in install instructions are not pinned, so if the Arrow project has moved ahead and we haven't, grab the binaries from [here](http://arrow.apache.org/release/2.0.0.html). 

There are 2 packages on the page you need to install to use arrow from Dlang. 
* Arrow For C++
* Arrow For Glib (C)

Check post installation if the package config files are installed correctly, to link at runtime. 
* `pkg-config --libs arrow`
* `pkg-config --libs arrow-glib`

The folder `darrow` hosts helper classes/methods to work with the Arrow C API. The build/run incantation you'd need for projects using this is: 
`-I={path-to-this-repo} -L-larrow-glib -L-lgobject-2.0 -L-larrow -L-lglib-2.0`


### Arrow for Python
Install the arrow package with `pip install pyarrow==3.0.0`. There are a couple helper utilities in the folder `arrowpy` but the `pyarrow` package is easy enough to use directly. Look up the [documentation](https://arrow.apache.org/docs/python/).


## Workflow
This library persists data in the Arrow IPC file format so that when uncompressed, it can be directly memory mapped. An IPC format file appends metadata at the end for random access to make sense, hence if your writing process exits midway, the file can't be read back. The default compression format used is `zstd` since we use that elsewhere.  

* Dlang - users don't interact with any Arrow data structure directly since they need to be tracked and free-d to keep memory usage low.
  * Write a table one row at a time to (un)compressed file.
  * Read a table one row at a time from (un)compressed file.
  * Read/Write a tensor to (un)compressed file. 
  * Memory map an uncompressed file, and access any of its columns. 

* Python - Arrow tables, columns, tensors come with a `to_numpy`/`to_pandas` method. Memory mapping generally wouldn't work with pandas/numpy since they require contiguous data while the files we write from Dlang are chunked. 
  * Read/write a table from an (un)compressed file, all at once or one chunk at a time.
  * Read/Write a tensor to (un)compressed file. 
  * Use the pyarrow library directly to do anything. 
