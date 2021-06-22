package org.sv.flexobject.hadoop.mapreduce.input;


import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.List;

public abstract class ConfiguredInputFormat<K,V> extends InputFormat<K,V> {

    protected abstract InputConf<InputConf> makeInputConf();

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        InputConf conf = makeInputConf().from(context.getConfiguration());
        try {
            return conf.getSplitter().split(context.getConfiguration());
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to instantiate splitter", e);
        }
    }

    @Override
    public RecordReader<K, V> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        try {
            return makeInputConf()
                    .from(context.getConfiguration())
                    .getReader();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to instantiate record reader", e);
        }
    }
}
