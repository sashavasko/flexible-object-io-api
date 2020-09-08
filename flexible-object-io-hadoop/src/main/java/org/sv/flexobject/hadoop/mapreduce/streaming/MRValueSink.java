package org.sv.flexobject.hadoop.mapreduce.streaming;

import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.sv.flexobject.stream.Sink;

public class MRValueSink<TYPE_EXTERNAL> implements Sink<TYPE_EXTERNAL> {

    TaskInputOutputContext context;
    Object key = null;

    public MRValueSink(TaskInputOutputContext context) {
        this.context = context;
    }

    public void setKey(Object key) {
        this.key = key;
    }

    @Override
    public boolean put(TYPE_EXTERNAL value) throws Exception {
        context.write(key, value);
        return true;
    }

    @Override
    public boolean hasOutput() {
        return false;
    }
}
