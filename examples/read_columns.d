import std.stdio;
import std.string;
import std.array;
import std.range;

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

void main() 
{
    writeln("Reading table by memory mapping: ");
    auto table = new ArrowColumnReader!Book("/tmp/batch.arrow");
    
    auto colA = table.getColumn!"a"();
    auto colB = table.getColumn!"qtys"();

    int sum = 0;
    int[2][2] qtys;
    foreach (x; colA) {
        sum += x;
    }
    foreach (y; colB) {
        qtys = y;
    }    

    writeln("num of elements in column `a`: ", colA.getNumRows);
    writeln("Sum of column `a`: ", sum);
    writeln("last value in column `qtys`: ", qtys);
}