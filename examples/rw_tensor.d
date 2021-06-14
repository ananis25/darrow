import std.stdio;
import std.string;
import std.array;
import std.traits;
import core.exception: AssertError;

import darrow.typedefs;
import darrow.tensors;


void main() 
{
    auto arr = new uint[2000][200][400];
    
    uint idx = 1;
    for (int i=0; i < arr.length; i++) 
        for (int j=0; j < arr[i].length; j++)
            for (int k=0; k < arr[i][j].length; k++) {
                arr[i][j][k] = idx;
                idx = idx+1;
            }

    long[] shape = [2000, 200, 400];
    string[] names = ["a", "b", "c"];
    auto dataBuffer = flatten(arr);

    auto tensor = ArrowTensor!uint(dataBuffer, shape, names);
    auto writer = new ArrowTensorWriter!uint("/tmp/tense.arrow", false);
    writer.write(tensor);

    // try to read the tensor, first with an incorrect type
    try {
        auto _reader = new ArrowTensorReader!int("/tmp/tense.arrow", false);
        auto _tensorRead = _reader.read();
    } catch (AssertError e) {
        writeln("Expected error: ", e.msg);
    }
    
    // read with the correct data type
    auto reader = new ArrowTensorReader!uint("/tmp/tense.arrow", false);
    auto tensorRead = reader.read();
    
    auto readBuffer = tensorRead.getDataBuffer();
    assert (dataBuffer.length == readBuffer.length);
    assert (dataBuffer == readBuffer);

    int i = 0;
    foreach(x; readBuffer)
        i += 1;
    writeln(i);    
}