package org.sv.flexobject.hadoop.mapreduce.input.mongo.oplog;

import org.bson.BsonTimestamp;
import org.sv.flexobject.hadoop.mapreduce.input.mongo.MongoSplit;

public class OplogSplit extends MongoSplit {
    protected String shardName;
    protected Integer lastTimestampSeconds;
    protected Integer getLastTimestampInc;

    public String getShardName() {
        return shardName;
    }

    public void setShardName(String shardName) {
        this.shardName = shardName;
    }

    public void setLastTimestamp(BsonTimestamp lastTimestamp){
        if (lastTimestamp != null) {
            lastTimestampSeconds = lastTimestamp.getTime();
            getLastTimestampInc = lastTimestamp.getInc();
        } else
            lastTimestampSeconds = null;
    }

    public BsonTimestamp getLastTimestamp(){
        if (lastTimestampSeconds == null)
            return null;
        return new BsonTimestamp(lastTimestampSeconds, getLastTimestampInc == null ? 0 : getLastTimestampInc);
    }
}
