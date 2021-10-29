package org.sv.flexobject.hadoop.mapreduce.input.mongo.splitters;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.CountOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.InputSplit;
import org.sv.flexobject.hadoop.mapreduce.input.ConfiguredInputFormat;
import org.sv.flexobject.hadoop.mapreduce.input.InputConf;
import org.sv.flexobject.hadoop.mapreduce.input.InputConfOwner;
import org.sv.flexobject.hadoop.mapreduce.input.Splitter;
import org.sv.flexobject.hadoop.mapreduce.input.mongo.MongoInputConf;
import org.sv.flexobject.hadoop.mapreduce.input.mongo.MongoSplit;
import org.sv.flexobject.hadoop.mapreduce.input.split.ProxyInputSplit;
import org.sv.flexobject.mongo.connection.MongoConnection;
import org.sv.flexobject.util.InstanceFactory;

import java.util.ArrayList;
import java.util.List;

public abstract class MongoSplitter extends Configured implements Splitter, InputConfOwner {

    private MongoInputConf conf;

    public <T extends InputConf> T getInputConf(){
        return (T)conf.getClass().cast(conf);
    }

    @Override
    public void setConf(Configuration conf) {
        super.setConf(conf);
        if (conf != null)
            getInputConf().from(conf);
    }

    /**
     * Get a list of nonempty input splits only.
     *
     * @param splits a list of input splits
     * @return a new list of nonempty input splits
     */
    public List<InputSplit> filterEmptySplits(
            final List<InputSplit> splits) throws Exception {
        List<InputSplit> results = new ArrayList<InputSplit>();

        MongoInputConf inputConf = getInputConf();
        try(MongoConnection connection = inputConf.getMongo()) {
            MongoCollection collection = connection.getCollection(conf.getCollectionName());
            for (InputSplit split : splits) {
                MongoSplit mis = ((ProxyInputSplit) split).getData();
                long estimatedSize = mis.getLength(collection, conf.getEstimateSizeLimit(), conf.getEstimateTimeLimitMicros());
                if (estimatedSize == 0)
                    estimatedSize = mis.getLength(collection, 1);
                if (estimatedSize > 0) {
                    mis.setEstimatedLength(estimatedSize);
                    ConfiguredInputFormat.logger.debug("Added non-empty split: " + mis.toString());
                    results.add(split);
                } else
                    ConfiguredInputFormat.logger.debug("Dropped empty split: " + mis.toString());
            }
        }
        return results;
    }
}
