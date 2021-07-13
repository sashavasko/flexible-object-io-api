package org.sv.flexobject.hadoop.mapreduce.input.mongo.splitters;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.CountOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.InputSplit;
import org.sv.flexobject.hadoop.mapreduce.input.Splitter;
import org.sv.flexobject.hadoop.mapreduce.input.mongo.MongoInputConf;
import org.sv.flexobject.hadoop.mapreduce.input.mongo.MongoSplit;
import org.sv.flexobject.mongo.connection.MongoConnection;
import org.sv.flexobject.util.InstanceFactory;

import java.util.ArrayList;
import java.util.List;

public abstract class MongoSplitter extends Configured implements Splitter {

    protected MongoInputConf conf;

    public MongoSplitter() {
        conf = makeConf();
    }

    public MongoInputConf makeConf(){
        return InstanceFactory.get(MongoInputConf.class);
    }

    @Override
    public void setConf(Configuration conf) {
        super.setConf(conf);
        if (conf != null)
            this.conf.from(conf);
    }

    /**
     * Get a list of nonempty input splits only.
     *
     * @param splits a list of input splits
     * @return a new list of nonempty input splits
     */
    public List<InputSplit> filterEmptySplits(
            final List<InputSplit> splits) throws Exception {
        List<InputSplit> results = new ArrayList<InputSplit>(splits.size());

        try(MongoConnection connection = conf.getMongo()) {
            MongoCollection collection = connection.getCollection(conf.getCollectionName());
            CountOptions countOptions = new CountOptions().limit(1);
            for (InputSplit split : splits) {
                MongoSplit mis = (MongoSplit) split;
                if (collection.countDocuments(mis.getQuery(), countOptions) > 0) {
                    results.add(mis);
                }
            }
        }
        return results;
    }
}
