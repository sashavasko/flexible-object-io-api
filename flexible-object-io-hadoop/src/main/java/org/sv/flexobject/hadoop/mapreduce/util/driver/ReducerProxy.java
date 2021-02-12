package org.sv.flexobject.hadoop.mapreduce.util.driver;


import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerProxy extends Reducer<WritableComparable, Writable, WritableComparable, Writable> {

    IReducerHelper helper;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try {
            helper = context.getConfiguration().getClass(IReducerHelper.REDUCER_HELPER_CONFIG, null, IReducerHelper.class).newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        helper.setupReducer(context);
    }

    @Override
    protected void reduce(WritableComparable key, Iterable<Writable> values, Context context) throws IOException, InterruptedException {
        helper.reduce(key, values, context);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        helper.cleanupReducer(context);
    }
}
