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
                "sv.input.mongo.oplog.connection.name",
                "sv.input.mongo.oplog.db.name",
                "sv.input.mongo.oplog.collection.name",
                "sv.input.mongo.oplog.estimate.size.limit",
                "sv.input.mongo.oplog.estimate.time.limit.micros",
                "sv.input.mongo.oplog.builder.class",
                "sv.input.mongo.oplog.schema",
                "sv.input.mongo.oplog.splitter.class",
                "sv.input.mongo.oplog.reader.class",
                "sv.input.mongo.oplog.source.builder.class",
                "sv.input.mongo.oplog.split.ops",
                "sv.input.mongo.oplog.split.timestamp.folder",
                "sv.input.mongo.oplog.start.timestamp",
                "sv.input.mongo.oplog.max.seconds.to.extract"
        );
        List<String> actualSettings = conf.listSettings();

        assertEquals(expectedSettings, actualSettings);
    }

    @Test
    public void limitTimestamp() {
        conf.maxSecondsToExtract = 100;
        BsonTimestamp expectedTimestamp = new BsonTimestamp((int)(System.currentTimeMillis()/1000)-100, 0);
        assertEquals(expectedTimestamp, conf.limitTimestamp(null));

        long tsMillis = System.currentTimeMillis()-20000;
        conf.startTimestamp = new Timestamp(tsMillis);
        expectedTimestamp = new BsonTimestamp((int)(tsMillis/1000), 0);
        assertEquals(expectedTimestamp, conf.limitTimestamp(null));
    }
}