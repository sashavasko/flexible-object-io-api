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
                "compressorList",
                "timeout",
                "username",
                "database",
                "readPreference",
                "hosts",
                "localThresholdMillis",
                "mode",
                "requiredClusterType",
                "requiredReplicaSetName",
                "serverSelectionTimeoutMillis",
                "serverSelector",
                "srvHost",
                "srvMaxHosts",
                "connectTimeoutMillis",
                "readTimeoutMillis",
                "receiveBufferSize",
                "sendBufferSize",
                "maintenanceFrequencyMillis",
                "maintenanceInitialDelayMillis",
                "maxConnectionIdleTimeMillis",
                "maxConnectionLifeTimeMillis",
                "maxWaitTimeMillis",
                "maxSize",
                "minSize",
                "heartbeatFrequencyMillis",
                "minHeartbeatFrequencyMillis");
        List<String> actualSettings = conf.listSettings();
        assertEquals(expectedSettings, actualSettings);
    }

    @Test
    public void fromProperties() throws Exception {
        Properties connectionProps = new Properties();
        connectionProps.setProperty("url", "mongodb://foo:bar@mongohost");
        connectionProps.setProperty("maxSize", "100");
        conf.from(connectionProps);
        assertEquals("mongodb://foo:bar@mongohost", conf.url);
        assertEquals(100, (int)conf.maxSize);
    }
}