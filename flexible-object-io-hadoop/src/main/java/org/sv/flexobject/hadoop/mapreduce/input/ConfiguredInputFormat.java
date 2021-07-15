package org.sv.flexobject.hadoop.mapreduce.input;


import org.apache.hadoop.mapreduce.*;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

public abstract class ConfiguredInputFormat<K,V> extends InputFormat<K,V> {
    protected static Logger logger = Logger.getLogger(ConfiguredInputFormat.class);
    protected abstract InputConf<InputConf> makeInputConf();

    @Override
    public List<InputSplit> getSplits(JobContext context) {
        InputConf conf = makeInputConf().from(context.getConfiguration());
        try {
            return conf.getSplitter().split(context.getConfiguration());
        } catch (Exception e) {
            throw conf.runtimeException(logger, "Failed to instantiate splitter", e);
        }
    }

    @Override
    public RecordReader<K, V> createRecordReader(InputSplit split, TaskAttemptContext context) {
        InputConf conf = makeInputConf().from(context.getConfiguration());
        return conf.getReader();
    }
}
