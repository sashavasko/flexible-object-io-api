package org.sv.flexobject.hadoop.mapreduce.streaming;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.sv.flexobject.stream.Sink;

import java.util.function.Function;

public class MRKeySink<TYPE_INTERNAL,TYPE_EXTERNAL> implements Sink<TYPE_INTERNAL> {

    TaskInputOutputContext context;
    Object value = NullWritable.get();
    Function<TYPE_INTERNAL, TYPE_EXTERNAL> converter = null;

    public MRKeySink(TaskInputOutputContext context) {
        this.context = context;
    }

    public MRKeySink(TaskInputOutputContext context, Function<TYPE_INTERNAL, TYPE_EXTERNAL> converter) {
        this.context = context;
        this.converter = converter;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    @Override
    public boolean put(TYPE_INTERNAL key) throws Exception {
        context.write(converter == null ? key: converter.apply(key), value);
        return true;
    }

    @Override
    public boolean hasOutput() {
        return false;
    }
}
