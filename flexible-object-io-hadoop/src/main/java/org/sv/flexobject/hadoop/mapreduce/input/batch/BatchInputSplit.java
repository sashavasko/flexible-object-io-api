package org.sv.flexobject.hadoop.mapreduce.input.batch;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.sv.flexobject.hadoop.utils.IConfigured;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BatchInputSplit extends InputSplit implements IConfigured, Writable {

    protected long batchSize;
    protected long batchPerSplit;

    long startKey;

    public BatchInputSplit(){}

    @Override
    public void reconfigure() {
        BatchInputConf conf = new BatchInputConf().from((getConf()));
        batchSize = conf.getSize();
        batchPerSplit = conf.getBatchesPerSplit();
    }

    public void setStartKey(long startKey) {
        this.startKey = startKey;
    }

    public long getStartKey() {
        return startKey;
    }

    public long getBatchSize() {
        return batchSize;
    }

    public long getBatchPerSplit() {
        return batchPerSplit;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return batchPerSplit;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[0];
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(batchSize);
        out.writeLong(batchPerSplit);
        out.writeLong(startKey);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        batchSize = in.readLong();
        batchPerSplit = in.readLong();
        startKey = in.readLong();
    }
}
