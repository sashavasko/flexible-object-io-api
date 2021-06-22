package org.sv.flexobject.hadoop.mapreduce.input.batch;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;
import org.sv.flexobject.hadoop.mapreduce.input.ConfiguredInputFormat;
import org.sv.flexobject.hadoop.mapreduce.input.InputConf;
import org.sv.flexobject.util.InstanceFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BatchInputFormat<VT> extends ConfiguredInputFormat<LongWritable, VT> {

    public static class Long extends BatchInputFormat<LongWritable>{}
    public static class Text extends BatchInputFormat<org.apache.hadoop.io.Text>{}
    public static class Double extends BatchInputFormat<DoubleWritable>{}

    @Override
    protected InputConf makeInputConf() {
        return InstanceFactory.get(BatchInputConf.class);
    }
}