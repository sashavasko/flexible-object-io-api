package org.sv.flexobject.mongo;

import com.mongodb.Tag;
import com.mongodb.TagSet;
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

    @Test
    public void tagsParsing() {
        conf.tags = Arrays.asList("dc:TXD,rack:cdd","dc:TXD,rack:add","dc:TXD,rack:ddd","dc_TXD,rack_bdd");

        List<TagSet> compiledTags = conf.compileTags();
        List<TagSet> expectedTags = Arrays.asList(new TagSet(Arrays.asList(new Tag("dc", "TXD"), new Tag("rack", "cdd"))),
                new TagSet(Arrays.asList(new Tag("dc", "TXD"), new Tag("rack", "add"))),
                new TagSet(Arrays.asList(new Tag("dc", "TXD"), new Tag("rack", "ddd"))),
                new TagSet(Arrays.asList(new Tag("dc", "TXD"), new Tag("rack", "bdd")))
        );

        assertEquals(expectedTags, compiledTags);
        conf.tags = Arrays.asList("dc:TXD");

        compiledTags = conf.compileTags();
        expectedTags = Arrays.asList(new TagSet(Arrays.asList(new Tag("dc", "TXD"))));

        assertEquals(expectedTags, compiledTags);
    }

}