package org.sv.flexobject.hadoop.mapreduce.input.mongo.oplog;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.sv.flexobject.hadoop.mapreduce.input.SourceRecordReader;
import org.sv.flexobject.hadoop.mapreduce.input.split.ProxyInputSplit;

import java.io.IOException;
import java.sql.Timestamp;

public abstract class OplogRecordReader<K> extends SourceRecordReader<K, Document, ProxyInputSplit> {
    public static final Logger logger = Logger.getLogger(OplogRecordReader.class);

    BsonTimestamp lastTimestamp = null;
    BsonTimestamp newLastTimestamp;
    String shardName;

    public static class NoKey extends OplogRecordReader<Void>{

        @Override
        protected Void extractKeyFromValue(Document value) {
            return null;
        }
    }

    public static class OId extends OplogRecordReader<BytesWritable>{

        BytesWritable key = new BytesWritable();

        @Override
        protected BytesWritable extractKeyFromValue(Document value) {
            Document doc = value.get("o", Document.class);
            key.setSize(0);
            if (doc != null) {
                ObjectId oid = doc.getObjectId("_id");
                if (oid != null) {
                    key.set(oid.toByteArray(), 0, 12);
                }
            }
            return key;
        }
    }

    @Override
    protected void setupInput(ProxyInputSplit split, TaskAttemptContext context) throws IOException {
        super.setupInput(split, context);
        OplogSplit oplogSplit = split.getData();
        lastTimestamp = oplogSplit.getLastTimestamp();
        shardName = oplogSplit.getShardName();
        OplogInputConf inputConf = getInputConf();
        if (lastTimestamp == null)
            lastTimestamp = inputConf.limitTimestamp(loadTimestamp());
        long time = lastTimestamp.getTime();
        Timestamp ts = new Timestamp(time*1000l);
        logger.info(inputConf.addDiagnostics("Limiting extract to timestamp " + ts.toString()));
    }

    public BsonTimestamp getLastTimestamp() {
        return lastTimestamp;
    }

    public String getShardName() {
        return shardName;
    }

    public void setNewLastTimestamp(BsonTimestamp newLastTimestamp) {
        this.newLastTimestamp = newLastTimestamp;
    }

    @Override
    protected boolean isValidRecord(Document record) {
        BsonTimestamp ts = record.get("ts", BsonTimestamp.class);
        if (newLastTimestamp == null)
            setNewLastTimestamp(ts);
        return ts.compareTo(lastTimestamp) > 0;
    }

    public void saveTimestamp() throws IOException {
        if (lastTimestamp != null) {
            String formattedTs = lastTimestamp.getTime() + "-" + lastTimestamp.getInc();
            OplogInputConf inputConf = getInputConf();
            Path tsPath = new Path(new Path(inputConf.getSplitTimestampFolder(), getShardName()), formattedTs);
            FileSystem.get(getConf()).createNewFile(tsPath);
        }

    }

    public BsonTimestamp loadTimestamp(){
        try {
            BsonTimestamp mostRecent = null;
            OplogInputConf inputConf = getInputConf();
            RemoteIterator<LocatedFileStatus> fileStatusListIterator = FileSystem.get(getConf())
                    .listFiles(new Path(inputConf.getSplitTimestampFolder(), getShardName()), false);
                while (fileStatusListIterator.hasNext()) {
                    LocatedFileStatus lfs = fileStatusListIterator.next();
                    String name = lfs.getPath().getName();
                    String[] parts = name.split("-");
                    int time = Integer.valueOf(parts[0]);
                    int inc = Integer.valueOf(parts[1]);
                    BsonTimestamp ts = new BsonTimestamp(time, inc);
                    if (mostRecent == null || mostRecent.compareTo(ts) < 0)
                        mostRecent = ts;

                }
            if (mostRecent != null)
                logger.info("Last Timestamp for shard " + getShardName() + " is " + mostRecent.toString());
            else
                mostRecent = new BsonTimestamp();
            return mostRecent;
        } catch (Exception e) {
            logger.warn("failed to obtain last Timestamp for shard " + getShardName(), e);
            return null;
        }
    }

    @Override
    public void close() throws IOException {
        saveTimestamp();
        super.close();
    }
}
