package org.sv.flexobject.hadoop.mapreduce.input.mongo;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.sv.flexobject.hadoop.mapreduce.input.mongo.splitters.MongoSplitter;
import org.sv.flexobject.hadoop.mapreduce.input.split.InputSplitImpl;
import org.sv.flexobject.hadoop.mapreduce.input.split.ProxyInputSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class ShardSplitter extends MongoSplitter {

    public ShardSplitter() {
    }

    @Override
    public List<InputSplit> split(Configuration conf) throws IOException {
        setConf(conf);

        List<InputSplit> splits = new ArrayList<>();
        ShardedInputConf inputConf = getInputConf();

        inputConf.getShards().forEach((name, hosts) -> {
            splits.add(new ProxyInputSplit(makeSplit((String)name, (String)hosts)));
        });

        return splits;
    }

    protected abstract InputSplitImpl makeSplit(String name, String hosts);
}
