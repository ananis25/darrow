import std.stdio;
import std.string;
import std.array;

import darrow.typedefs;
import darrow.table;


struct Book
{
    int a;
    uint b;
    bool c;
    long[2] prices;
    int[2][2] qtys;
}


void main(string[] argv) 
{
    assert (argv.length == 2, "pass in true for compressed write, else false");
    auto compress = (argv[1] == "true") ? true : false;

    auto reader = new ArrowStreamReader!Book("/tmp/batch.arrow", compress);
    int i = 0;
    Book current;
    foreach (Book row; reader) {
        current = row;
        i += 1;
    }
    writeln("Reading table as a stream of rows: ");
    writeln("num rows: ", i);
    writeln("last row: ", current);
}