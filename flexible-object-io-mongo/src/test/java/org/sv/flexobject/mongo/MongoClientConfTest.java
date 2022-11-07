package org.sv.flexobject.mongo;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class MongoClientConfTest {
    MongoClientConf conf = new MongoClientConf();

    @Test
    public void listSettings() {
        List<String> expectedSettings = Arrays.asList("url",
                        "tags",
                        "compressor.list",
                        "timeout",
                        "username",
                        "database",
                        "read.preference",
                        "hosts",
                        "local.threshold.millis",
                        "mode",
                        "required.cluster.type",
                        "required.replica.set.name",
                        "server.selection.timeout.millis",
                        "server.selector",
                        "srv.host",
                        "srv.max.hosts",
                        "connect.timeout.millis",
                        "read.timeout.millis",
                        "receive.buffer.size",
                        "send.buffer.size",
                        "maintenance.frequency.millis",
                        "maintenance.initial.delay.millis",
                        "max.connection.idle.time.millis",
                        "max.connection.life.time.millis",
                        "max.wait.time.millis",
                        "max.size",
                        "min.size",
                        "heartbeat.frequency.millis",
                        "min.heartbeat.frequency.millis");
        List<String> actualSettings = conf.listSettings();
        assertEquals(expectedSettings, actualSettings);
    }

    @Test
    public void fromProperties() throws Exception {
        Properties connectionProps = new Properties();
        connectionProps.setProperty("url", "mongodb://foo:bar@mongohost");
        connectionProps.setProperty("max.size", "100");
        conf.from(connectionProps);
        assertEquals("mongodb://foo:bar@mongohost", conf.url);
        assertEquals(100, (int)conf.maxSize);
    }
}