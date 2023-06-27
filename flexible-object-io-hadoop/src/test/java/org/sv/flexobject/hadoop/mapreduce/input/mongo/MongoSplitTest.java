package org.sv.flexobject.hadoop.mapreduce.input.mongo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.CountOptions;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.sv.flexobject.hadoop.streaming.parquet.ParquetSchema;
import org.sv.flexobject.util.InstanceFactory;

import java.io.*;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class MongoSplitTest {

    String queryJson = "{\"id\":\"foo\"}";
    String projectionJson = "{\"id\":1}";;
    String sortJson = "{\"noid\":1}";

    Integer limit = 77;
    Integer skip = 7;
    Long estimatedLength = 1l;
    boolean noTimeout = true;

    @Mock
    MongoCollection collection;

    @Mock
    CountOptions countOptions;

    @Mock
    DataOutput output;

    @Mock
    DataInput input;


    MongoSplit split;

    @Before
    public void setUp() throws Exception {
        split = new MongoSplit(queryJson, projectionJson, sortJson, limit, skip, noTimeout);
    }

    @After
    public void tearDown() throws Exception {
        InstanceFactory.reset();
    }

    @Test
    public void getQuery() {
        assertNull(new MongoSplit().getQuery());

        Bson query = split.getQuery();
        Bson expectedQuery = new Document("id", "foo");

        assertEquals(expectedQuery, query);
    }

    @Test
    public void getLengthFromCollection() {
        InstanceFactory.set(CountOptions.class, countOptions);
        Bson query = split.getQuery();

        doReturn(12l).when(collection).countDocuments(query, countOptions);

        assertEquals(12l, split.getLength(collection, 123));

        verify(countOptions).limit(limit);
        verify(countOptions).skip(skip);
    }

    @Test
    public void getLengthWithMaxTime() {
        InstanceFactory.set(CountOptions.class, countOptions);
        Bson query = split.getQuery();

        doReturn(17l).when(collection).countDocuments(query, countOptions);

        assertEquals(17l, split.getLength(collection, 2, 777));

        verify(countOptions).limit(2);
        verify(countOptions).skip(skip);
        verify(countOptions).maxTime(777, TimeUnit.MICROSECONDS);
    }

    @Test
    public void getLength() throws IOException, InterruptedException {
        split.setEstimatedLength(8989l);

        assertEquals(8989l, split.getLength());
    }

    @Test
    public void getLocations() throws IOException, InterruptedException {
        assertArrayEquals(new String[0], split.getLocations());
    }

    @Test
    public void write() throws Exception {
        split.write(output);

        byte[] jsonBytes = split.toJsonBytes();

        verify(output).writeInt(jsonBytes.length);
        verify(output).write(jsonBytes);
    }

    @Test
    public void readFields() throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutput output = new DataOutputStream(byteArrayOutputStream);

        split.write(output);

        MongoSplit split2 = new MongoSplit();

        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
        split2.readFields(new DataInputStream(byteArrayInputStream));

        assertEquals(split, split2);
    }

    @Test
    public void hasQuery() {
        assertFalse(new MongoSplit().hasQuery());

        assertTrue(split.hasQuery());
    }

    @Test
    public void hasProjection() {
        assertFalse(new MongoSplit().hasProjection());

        assertTrue(split.hasProjection());
    }

    @Test
    public void hasSort() {
        assertFalse(new MongoSplit().hasSort());

        assertTrue(split.hasSort());
    }

    @Test
    public void hasLimit() {
        assertFalse(new MongoSplit().hasLimit());

        assertTrue(split.hasLimit());
    }

    @Test
    public void getSort() {

        assertNull(new MongoSplit().getSort());

        Bson sort = split.getSort();
        Bson expectedSort = new Document("noid", 1);

        assertEquals(expectedSort, sort);
    }

    @Test
    public void getLimit() {
        assertNull(new MongoSplit().getLimit());

        assertEquals(limit, split.getLimit());
    }

    @Test
    public void getSkip() {
        assertNull(new MongoSplit().getSkip());

        assertEquals(skip, split.getSkip());
    }

    @Test
    public void isNotimeout() {
        assertFalse(new MongoSplit().isNotimeout());
        assertTrue(split.isNotimeout());
    }

    @Test
    public void hasSkip() {
        assertFalse(new MongoSplit().hasSkip());

        assertTrue(split.hasSkip());
    }

    @Test
    public void getProjection() {
        assertNull(new MongoSplit().getProjection());
        Bson projection = split.getProjection();
        Bson expectedProjection = new Document("id", 1);

        assertEquals(expectedProjection, projection);
    }

    @Test
    public void hashCodeOnlyQuery() throws JsonProcessingException {
        ObjectNode query = JsonNodeFactory.instance.objectNode();
        query.put("_id", 1);
        assertEquals(query.hashCode(), new MongoSplit(query).hashCode());
    }

    @Test
    public void parquetSchema() {
        assertEquals("message org.sv.flexobject.hadoop.mapreduce.input.mongo.MongoSplit {\n" +
                "  optional binary queryJson (JSON);\n" +
                "  optional binary projectionJson (JSON);\n" +
                "  optional binary sortJson (JSON);\n" +
                "  optional int32 limit;\n" +
                "  optional int32 skip;\n" +
                "  optional boolean noTimeout;\n" +
                "  optional binary hosts (UTF8);\n" +
                        "  optional binary dbName (UTF8);\n" +
                        "  optional binary collectionName (UTF8);\n" +
                "  optional int64 estimatedLength;\n" +
                "}\n", ParquetSchema.forClass(MongoSplit.class).toString());
    }

    @Test
    public void objectIdQuery() {
        //"609285a90000000000000000"}}, {"_id": {"$lt": "6092868a0000000000000000
        Bson query = Filters.and(Filters.gte("_id", new ObjectId("609285a90000000000000000")),Filters.lt("_id", new ObjectId("6092868a0000000000000000")));

        JsonNode json = MongoSplit.bson2json(query);
        assertEquals(query.toBsonDocument().toJson().replace(" ", ""), json.toString());
    }
}