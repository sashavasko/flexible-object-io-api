package org.sv.flexobject.hadoop.streaming.avro.write;

import org.junit.Test;
import org.sv.flexobject.hadoop.streaming.avro.AvroSchema;
import org.sv.flexobject.hadoop.streaming.avro.read.AvroSource;
import org.sv.flexobject.hadoop.streaming.testdata.TestDataWithSubSchema;
import org.sv.flexobject.hadoop.streaming.testdata.TestDataWithSubSchemaInCollection;
import org.sv.flexobject.hadoop.streaming.testdata.levelone.ObjectWithNestedObject;

import java.io.ByteArrayOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AvroSinkTest {

    @Test
    public void writeReadVerySimple() throws Exception {
        ObjectWithNestedObject testData = ObjectWithNestedObject.random();

        System.out.println(AvroSchema.forClass(ObjectWithNestedObject.class));

        AvroSink<ObjectWithNestedObject> sink = new AvroSink<>();
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        sink.builder().forOutput(os).withSchema(ObjectWithNestedObject.class);

        sink.put(testData);
        sink.close();

        AvroSource<ObjectWithNestedObject> source = new AvroSource<>(ObjectWithNestedObject.class);
        source.builder().forInput(os.toByteArray());

        ObjectWithNestedObject result = source.get();

        assertEquals(testData, result);
    }

    @Test
    public void writeReadSimple() throws Exception {
        TestDataWithSubSchema testData = TestDataWithSubSchema.random(true);

        System.out.println(AvroSchema.forClass(TestDataWithSubSchema.class));

        AvroSink<TestDataWithSubSchema> sink = new AvroSink<>();
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        sink.builder().forOutput(os).withSchema(TestDataWithSubSchema.class);

        sink.put(testData);
        sink.close();

        AvroSource<TestDataWithSubSchema> source = new AvroSource<>(TestDataWithSubSchema.class);
        source.builder().forInput(os.toByteArray());

        TestDataWithSubSchema result = source.get();

        assertTrue(testData.equals(result));
    }

    @Test
    public void writeRead() throws Exception {
        TestDataWithSubSchemaInCollection testData = TestDataWithSubSchemaInCollection.random(true);

        System.out.println(AvroSchema.forClass(TestDataWithSubSchemaInCollection.class));

        AvroSink<TestDataWithSubSchemaInCollection> sink = new AvroSink<>();
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        sink.builder().forOutput(os).withSchema(TestDataWithSubSchemaInCollection.class);

        sink.put(testData);
        sink.close();

        AvroSource<TestDataWithSubSchemaInCollection> source = new AvroSource<>(TestDataWithSubSchemaInCollection.class);
        source.builder().forInput(os.toByteArray());

        TestDataWithSubSchemaInCollection result = source.get();

        assertEquals(testData, result);
    }
}