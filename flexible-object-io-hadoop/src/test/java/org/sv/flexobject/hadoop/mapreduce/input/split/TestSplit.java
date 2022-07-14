package org.sv.flexobject.hadoop.mapreduce.input.split;


import org.sv.flexobject.hadoop.StreamableWritableImpl;

import java.io.IOException;

public class TestSplit extends StreamableWritableImpl implements InputSplitImpl {

    Long length;

    public TestSplit() {
    }

    public TestSplit(Long length) {
        this.length = length;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return length;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[0];
    }
}
