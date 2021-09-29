package org.sv.flexobject.hadoop.mapreduce.input.split.timerange;

import org.apache.hadoop.io.LongWritable;
import org.sv.flexobject.hadoop.mapreduce.input.ConfiguredInputFormat;
import org.sv.flexobject.hadoop.mapreduce.input.InputConf;
import org.sv.flexobject.util.InstanceFactory;

public class TimeRangeInputFormat extends ConfiguredInputFormat<LongWritable, LongWritable> {
    @Override
    protected InputConf makeInputConf() {
        return InstanceFactory.get(TimeRangeConf.class);
    }
}
