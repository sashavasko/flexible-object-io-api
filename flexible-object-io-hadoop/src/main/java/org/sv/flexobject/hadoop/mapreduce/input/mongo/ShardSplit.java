package org.sv.flexobject.hadoop.mapreduce.input.mongo;

public class ShardSplit extends MongoSplit {
    protected String shardName;

    public String getShardName() {
        return shardName;
    }

    public void setShardName(String shardName) {
        this.shardName = shardName;
    }
}
