package org.sv.flexobject.hadoop.mapreduce.input.mongo.oplog;

import org.bson.BsonTimestamp;
import org.sv.flexobject.hadoop.mapreduce.input.mongo.ShardSplit;

public class OplogSplit extends ShardSplit {
    protected Integer lastTimestampSeconds;
    protected Integer getLastTimestampInc;

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
