package org.sv.flexobject.hadoop.mapreduce.input.batch;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.sv.flexobject.hadoop.utils.IConfigured;
import org.sv.flexobject.util.InstanceFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BatchInputSplit extends InputSplit implements IConfigured, Writable {

    Long startKey;
    protected long batchSize;
    protected long batchPerSplit;

    public BatchInputSplit(){}

    @Override
    public void reconfigure() {
        BatchInputConf conf = InstanceFactory.get(BatchInputConf.class);
        conf.from(getConf());
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

    public void setBatchPerSplit(long batchPerSplit) {
        this.batchPerSplit = batchPerSplit;
    }

    public long getBatchPerSplit() {
        return batchPerSplit;
    }

    @Override
    public long getLength() {
        return batchPerSplit;
    }

    @Override
    public String[] getLocations() {
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BatchInputSplit that = (BatchInputSplit) o;
        return batchSize == that.batchSize && batchPerSplit == that.batchPerSplit && startKey.equals(that.startKey);
    }

    @Override
    public int hashCode() {
        return startKey.hashCode();
    }

    @Override
    public String toString() {
        return "BatchInputSplit{" +
                "startKey=" + startKey +
                ", batchSize=" + batchSize +
                ", batchPerSplit=" + batchPerSplit +
                '}';
    }
}
