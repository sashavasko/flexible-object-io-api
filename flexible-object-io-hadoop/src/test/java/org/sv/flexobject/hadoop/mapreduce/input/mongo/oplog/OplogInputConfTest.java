package org.sv.flexobject.hadoop.mapreduce.input.mongo.oplog;

import org.bson.BsonTimestamp;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class OplogInputConfTest {

    OplogInputConf conf = new OplogInputConf();

    @Test
    public void listSettings() {
        List<String> expectedSettings = Arrays.asList(
                "cfx.input.mongo.oplog.connection.name",
                "cfx.input.mongo.oplog.db.name",
                "cfx.input.mongo.oplog.collection.name",
                "cfx.input.mongo.oplog.estimate.size.limit",
                "cfx.input.mongo.oplog.estimate.time.limit.micros",
                "cfx.input.mongo.oplog.builder.class",
                "cfx.input.mongo.oplog.schema",
                "cfx.input.mongo.oplog.splitter.class",
                "cfx.input.mongo.oplog.reader.class",
                "cfx.input.mongo.oplog.source.builder.class",
                "cfx.input.mongo.oplog.split.ops",
                "cfx.input.mongo.oplog.split.timestamp.folder",
                "cfx.input.mongo.oplog.start.timestamp",
                "cfx.input.mongo.oplog.max.seconds.to.extract"
        );
        List<String> actualSettings = conf.listSettings();

        assertEquals(expectedSettings, actualSettings);
    }

    @Test
    public void limitTimestamp() {
        conf.maxSecondsToExtract = 100;
        BsonTimestamp expectedTimestamp = new BsonTimestamp((int)(System.currentTimeMillis()/1000)-100);
        assertEquals(expectedTimestamp, conf.limitTimestamp(null));

        long tsMillis = System.currentTimeMillis()-20000;
        conf.startTimestamp = new Timestamp(tsMillis);
        expectedTimestamp = new BsonTimestamp((int)(tsMillis/1000));
        assertEquals(expectedTimestamp, conf.limitTimestamp(null));
    }
}