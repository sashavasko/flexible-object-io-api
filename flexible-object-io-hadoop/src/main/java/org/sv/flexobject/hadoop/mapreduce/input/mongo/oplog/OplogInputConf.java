package org.sv.flexobject.hadoop.mapreduce.input.mongo.oplog;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.bson.BsonTimestamp;
import org.sv.flexobject.hadoop.mapreduce.input.mongo.MongoInputConf;
import org.sv.flexobject.hadoop.properties.HadoopPropertiesWrapper;
import org.sv.flexobject.properties.Namespace;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class OplogInputConf<SELF extends HadoopPropertiesWrapper> extends MongoInputConf<SELF> {
    public static final String SUBNAMESPACE = "oplog";
    
    String splitOps;
    String splitTimestampFolder;
    Timestamp startTimestamp;
    Integer maxSecondsToExtract;
    public static final Map<String, String> shards = new HashMap<>();

    public OplogInputConf() {
        super(SUBNAMESPACE);
    }

    public OplogInputConf(String child) {
        super(makeMyNamespace(getParentNamespace(OplogInputConf.class), SUBNAMESPACE), child);
    }

    public OplogInputConf(Namespace parent) {
        super(parent, SUBNAMESPACE);
    }

    public OplogInputConf(Namespace parent, String child) {
        super(parent, child);
    }


    public Map<String, String> getShards(){
        return shards;
    }

    public SELF addShard(String name, String hosts){
        shards.put(name, hosts);
        return (SELF) this;
    }

    @Override
    public SELF setDefaults() {
        super.setDefaults();
        splitterClass = OplogSplitter.class;
        readerClass = OplogRecordReader.NoKey.class;

        splitOps = "i,u,d";
        splitTimestampFolder = "oplog/lastTimestamp";
        return (SELF) this;
    }

    public String[] getSplitOps() {
        return StringUtils.isNotBlank(splitOps) ? splitOps.split(",") : new String[0];
    }

    public Path getSplitTimestampFolder() {
        return new Path(splitTimestampFolder);
    }

    BsonTimestamp limitTimestamp(BsonTimestamp lastSavedTimestamp){
        BsonTimestamp startTimestamp = this.startTimestamp == null ?
                new BsonTimestamp()
                : new BsonTimestamp((int)(this.startTimestamp.getTime()/1000l), 0);
        BsonTimestamp maxTimestamp = this.maxSecondsToExtract == null ?
                new BsonTimestamp()
                : new BsonTimestamp((int)(System.currentTimeMillis()/1000l) - this.maxSecondsToExtract, 0);
        BsonTimestamp limit = startTimestamp.compareTo(maxTimestamp) > 0 ? startTimestamp : maxTimestamp;
        if (lastSavedTimestamp == null){
            return limit;
        }
        return limit.compareTo(lastSavedTimestamp) > 0 ? limit : lastSavedTimestamp;
    }
}
