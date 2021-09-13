package org.sv.flexobject.hadoop.mapreduce.input.mongo.splitters;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.CountOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.InputSplit;
import org.sv.flexobject.hadoop.mapreduce.input.ConfiguredInputFormat;
import org.sv.flexobject.hadoop.mapreduce.input.Splitter;
import org.sv.flexobject.hadoop.mapreduce.input.mongo.MongoInputConf;
import org.sv.flexobject.hadoop.mapreduce.input.mongo.MongoSplit;
import org.sv.flexobject.mongo.connection.MongoConnection;
import org.sv.flexobject.util.InstanceFactory;

import java.util.ArrayList;
import java.util.List;

public abstract class MongoSplitter extends Configured implements Splitter {

    private MongoInputConf conf;

    public MongoInputConf makeConf(){
        return InstanceFactory.get(MongoInputConf.class);
    }

    protected MongoInputConf getInputConf(){
        if (conf == null) {
            conf = makeConf();
        }

        return conf;
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

        try(MongoConnection connection = getInputConf().getMongo()) {
            MongoCollection collection = connection.getCollection(conf.getCollectionName());
            for (InputSplit split : splits) {
                MongoSplit mis = (MongoSplit) split;
                long estimatedSize = mis.getLength(collection, conf.getEstimateSizeLimit(), conf.getEstimateTimeLimitMicros());
                if (estimatedSize == 0)
                    estimatedSize = mis.getLength(collection, 1);
                if (estimatedSize > 0) {
                    mis.setEstimatedLength(estimatedSize);
                    ConfiguredInputFormat.logger.debug("Added non-empty split: " + mis.toString());
                    results.add(mis);
                } else
                    ConfiguredInputFormat.logger.debug("Dropped empty split: " + mis.toString());
            }
        }
        return results;
    }
}
