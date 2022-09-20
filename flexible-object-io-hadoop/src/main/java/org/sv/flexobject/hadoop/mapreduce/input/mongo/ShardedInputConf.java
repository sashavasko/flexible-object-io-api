package org.sv.flexobject.hadoop.mapreduce.input.mongo;


import org.sv.flexobject.hadoop.mapreduce.input.mongo.oplog.OplogRecordReader;
import org.sv.flexobject.hadoop.mapreduce.input.mongo.oplog.OplogSplitter;
import org.sv.flexobject.hadoop.properties.HadoopPropertiesWrapper;
import org.sv.flexobject.properties.Namespace;

import java.util.HashMap;
import java.util.Map;

public class ShardedInputConf<SELF extends HadoopPropertiesWrapper> extends MongoInputConf<SELF> {
    public static final Map<String, String> shards = new HashMap<>();

    public ShardedInputConf(String subnamespace) {
        super((subnamespace));
    }

    public ShardedInputConf(Namespace namespace, String child) {
         super(namespace, child);
    }

    public Map<String, String> getShards(){
        return shards;
    }

    public SELF addShard(String name, String hosts){
        shards.put(name, hosts);
        return (SELF) this;
    }

    public SELF setShards(Map<String, String> shards){
        this.shards.clear();
        this.shards.putAll(shards);
        return (SELF) this;
    }

    @Override
    public SELF setDefaults() {
        super.setDefaults();
        splitterClass = OplogSplitter.class;
        readerClass = OplogRecordReader.NoKey.class;

        return (SELF) this;
    }
}
