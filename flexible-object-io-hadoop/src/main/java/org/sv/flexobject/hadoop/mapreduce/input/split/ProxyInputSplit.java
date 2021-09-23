package org.sv.flexobject.hadoop.mapreduce.input.split;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.sv.flexobject.util.InstanceFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ProxyInputSplit extends InputSplit implements Writable {

    InputSplitImpl data;

    public ProxyInputSplit() {
    }

    public ProxyInputSplit(InputSplitImpl data) {
        this.data = data;
    }

    public <T extends InputSplitImpl> T getData() {
        return (T)data.getClass().cast(data);
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return data.getLength();
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return data.getLocations();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(data.getClass().getName());
        data.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        String dataClassName = dataInput.readUTF();
        try {
            data = (InputSplitImpl) InstanceFactory.get(Class.forName(dataClassName));
            data.readFields(dataInput);
        } catch (ClassNotFoundException e) {
            throw new IOException("Cannot instantiate InputSplit data object", e);
        }
    }
}
