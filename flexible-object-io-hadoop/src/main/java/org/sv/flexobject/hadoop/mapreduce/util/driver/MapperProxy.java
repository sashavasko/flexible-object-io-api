package org.sv.flexobject.hadoop.mapreduce.util.driver;


import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperProxy extends Mapper<WritableComparable, Writable, WritableComparable, Writable> {

    IMapperHelper helper;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try {
            helper = context.getConfiguration().getClass(IMapperHelper.MAPPER_HELPER_CONFIG, null, IMapperHelper.class).newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        helper.setupMapper(context);
    }

    @Override
    protected void map(WritableComparable key, Writable value, Context context) throws IOException, InterruptedException {
        helper.map(key, value, context);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        helper.cleanupMapper(context);
    }
}
