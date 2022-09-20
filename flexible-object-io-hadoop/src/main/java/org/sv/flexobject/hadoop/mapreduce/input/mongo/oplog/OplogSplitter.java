package org.sv.flexobject.hadoop.mapreduce.input.mongo.oplog;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.bson.BsonTimestamp;
import org.bson.conversions.Bson;
import org.sv.flexobject.hadoop.mapreduce.input.mongo.MongoSplit;
import org.sv.flexobject.hadoop.mapreduce.input.mongo.ShardSplitter;
import org.sv.flexobject.hadoop.mapreduce.input.mongo.splitters.MongoSplitter;
import org.sv.flexobject.hadoop.mapreduce.input.split.InputSplitImpl;
import org.sv.flexobject.hadoop.mapreduce.input.split.ProxyInputSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OplogSplitter extends ShardSplitter {

    public OplogSplitter() {
    }

    protected BsonTimestamp getLastTimestamp(String shardName){ return null; }

    public InputSplitImpl makeSplit(String name, String hosts) {
        OplogInputConf inputConf = getInputConf();
        Bson query = Filters.and(
                Filters.eq("ns", inputConf.getDbName() + "." + inputConf.getCollectionName()),
                Filters.in("op", inputConf.getSplitOps()),
                Filters.exists("fromMigrate", false));
        Bson sort = Sorts.descending("$natural");

        OplogSplit split = MongoSplit.builder()
                .splitClass(OplogSplit.class)
                .query(query)
                .sort(sort)
                .db("local")
                .collection("oplog.rs")
                .hosts(hosts)
                .noTimeout()
                .length(100000)
                .build();
        split.setLastTimestamp(inputConf.limitTimestamp(getLastTimestamp(name)));
        split.setShardName(name);
        return split;
    }
}
