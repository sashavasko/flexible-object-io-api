package org.sv.flexobject.hadoop.mapreduce.input.split;

import org.apache.hadoop.io.Writable;

import java.io.IOException;

public interface InputSplitImpl extends Writable {
    long getLength() throws IOException, InterruptedException;

    String[] getLocations() throws IOException, InterruptedException;
}
