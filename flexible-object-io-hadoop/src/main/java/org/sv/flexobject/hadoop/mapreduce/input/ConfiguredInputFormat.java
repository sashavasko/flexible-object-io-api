package org.sv.flexobject.hadoop.mapreduce.input;


import org.apache.hadoop.mapreduce.*;
import org.apache.log4j.Logger;
import org.sv.flexobject.hadoop.HadoopTask;
import org.sv.flexobject.hadoop.mapreduce.input.split.InputSplitImpl;
import org.sv.flexobject.hadoop.mapreduce.input.split.ProxyInputSplit;
import org.sv.flexobject.util.InstanceFactory;

import java.io.IOException;
import java.util.List;

public class ConfiguredInputFormat<K,V> extends InputFormat<K,V> {
    public static final Logger logger = Logger.getLogger(ConfiguredInputFormat.class);

    protected InputConf<InputConf> makeInputConf(){
        return HadoopTask.getTaskConf().instantiateConf(InputConf.class);
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) {
        InputConf conf = makeInputConf().from(context.getConfiguration());
        try {
            logger.info("Creating splits using configuration: " + conf.toString());
            return conf.getSplitter().split(context.getConfiguration());
        } catch (Exception e) {
            throw conf.runtimeException(logger, "Failed to instantiate splitter", e);
        }
    }

    @Override
    public RecordReader<K, V> createRecordReader(InputSplit split, TaskAttemptContext context) {
        if (!HadoopTask.isConfigured()) {
            try {
                HadoopTask.configure(context.getConfiguration());
            } catch (Exception e) {
                throw HadoopTask.getTaskConf().runtimeException(logger, "Failed to initialize HadoopTask", e);
            }
        }
        InputConf conf = makeInputConf().from(context.getConfiguration());
        RecordReader reader = conf.getReader();

        logger.info(getClass().getName() + " created new RecordReader " + reader.getClass().getName() + " using configuration " + conf.getClass().getName() + " " + conf);
        if (split instanceof ProxyInputSplit){
            InputSplitImpl data = ((ProxyInputSplit) split).getData();
            logger.info("Input split:" + data);
        }

        return reader;
    }
}
