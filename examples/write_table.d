import std.stdio;
import std.string;
import std.array;

import darrow.typedefs;
import darrow.table;


struct Book
{
    int a = 10;
    uint b = 20;
    bool c = false;
    long[2] prices;
    int[2][2] qtys;
}


void main(string[] argv) 
{
    assert (argv.length == 2, "pass in true for compressed write, else false");
    auto compress = (argv[1] == "true") ? true : false;
    
    enum int SIZE = 500_000;
    auto books = new Book[SIZE];
    books[0 .. SIZE/2] = Book(1, 2, true, [5,7], [[1,2], [3,4]]);
    books[SIZE/2 .. SIZE] = Book(2, 1, false, [7,8], [[5,6], [7,8]]);

    auto writer = new ArrowStreamWriter!Book("/tmp/batch.arrow", compress);
    for(int i = 0; i < 10; i++) {
        foreach(row; books)
            writer.writeRow(row);
        writeln("iter: ", i);
    }
    writer.finish();
    writeln("writing finished");
}