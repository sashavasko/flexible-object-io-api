package org.sv.flexobject.hadoop.mapreduce.input.mongo;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.mongo.connection.MongoConnection;
import org.sv.flexobject.mongo.streaming.MongoBuilder;
import org.sv.flexobject.mongo.streaming.MongoDocumentSource;
import org.sv.flexobject.testdata.MapWithTypedKey;
import org.sv.flexobject.testdata.TestDataWithSubSchema;
import org.sv.flexobject.util.InstanceFactory;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class MongoInputConfTest {

    @Mock
    MongoConnection.Builder mockBuilder;

    @Mock
    MongoConnection mockConnection;

    @Mock
    MongoBuilder mockSourceBuilder;

    MongoInputConf conf;
    Configuration rawConf;

    @Before
    public void setUp() throws Exception {
        InstanceFactory.set(MongoConnection.Builder.class, mockBuilder);
        rawConf = new Configuration(false);
        conf = new MongoInputConf("test");
    }

    @After
    public void tearDown() throws Exception {
        InstanceFactory.reset();
    }

    @Test
    public void setDefaults() {
        assertSame(conf, conf.setDefaults());

        assertEquals(100000, conf.getEstimateSizeLimit());
        assertEquals(1000, conf.getEstimateTimeLimitMicros());
        assertTrue(conf.getSourceBuilder() instanceof MongoDocumentSource.Builder);
    }

    @Test
    public void namespace() {
        assertEquals("input.mongo", conf.getSubNamespace());

        conf = new MongoInputConf("foo.bar");

        assertEquals("foo.bar.input.mongo", conf.getNamespace());
    }

    @Test
    public void getConnectionName() {
        assertNull(conf.getConnectionName());

        rawConf.set("test.input.mongo.connection.name", "mongodb");
        conf.from(rawConf);

        assertEquals("mongodb", conf.getConnectionName());
    }

    @Test
    public void estimateLimits() {
        rawConf.setInt("test.input.mongo.estimate.size.limit", 700000);
        rawConf.setInt("test.input.mongo.estimate.time.limit.micros", 7000);
        conf.from(rawConf);

        assertEquals(700000, conf.getEstimateSizeLimit());
        assertEquals(7000, conf.getEstimateTimeLimitMicros());
    }

    @Test
    public void getDbName() {
        assertNull(conf.getDbName());

        rawConf.set("test.input.mongo.db.name", "mydb");
        conf.from(rawConf);

        assertEquals("mydb", conf.getDbName());
    }

    @Test
    public void getCollectionName() {
        assertNull(conf.getCollectionName());

        rawConf.set("test.input.mongo.collection.name", "mycollection");
        conf.from(rawConf);

        assertEquals("mycollection", conf.getCollectionName());
    }

    @Test
    public void getInputSchema() {
        assertNull(conf.getInputSchema());
        assertFalse(conf.hasSchema());

        rawConf.setClass("test.input.mongo.schema", TestDataWithSubSchema.class, StreamableWithSchema.class);
        conf.from(rawConf);

        assertSame(TestDataWithSubSchema.class, conf.getInputSchema());
        assertTrue(conf.hasSchema());
    }

    @Test
    public void getMongo() throws Exception {
        rawConf.set("test.input.mongo.connection.name", "mongodb");
        rawConf.set("test.input.mongo.db.name", "mydb");
        conf.from(rawConf);

        doReturn(mockBuilder).when(mockBuilder).forName("mongodb");
        doReturn(mockBuilder).when(mockBuilder).db("mydb");
        doReturn(mockConnection).when(mockBuilder).build();

        assertSame(mockConnection, conf.getMongo());

        verify(mockBuilder).forName("mongodb");
        verify(mockBuilder).db("mydb");
    }

    @Test
    public void getSourceMongoBuilder() {
        rawConf.set("test.input.mongo.source.builder.class", String.class.getName());
        rawConf.set("test.input.mongo.connection.name", "mongodb");
        rawConf.set("test.input.mongo.collection.name", "mycollection");
        rawConf.set("test.input.mongo.db.name", "mydb");
        rawConf.setClass("test.input.mongo.schema", MapWithTypedKey.class, StreamableWithSchema.class);
        conf.from(rawConf);

        InstanceFactory.set(String.class, mockSourceBuilder);

        doReturn(mockSourceBuilder).when(mockSourceBuilder).connection(anyString());
        doReturn(mockSourceBuilder).when(mockSourceBuilder).db(anyString());
        doReturn(mockSourceBuilder).when(mockSourceBuilder).collection(anyString());

        MongoBuilder actualBuilder = conf.getSourceBuilder();

        assertEquals(mockSourceBuilder, actualBuilder);
        verify(mockSourceBuilder).connection("mongodb");
        verify(mockSourceBuilder).db("mydb");
        verify(mockSourceBuilder).collection("mycollection");
        verify(mockSourceBuilder).schema(MapWithTypedKey.class);
    }
}