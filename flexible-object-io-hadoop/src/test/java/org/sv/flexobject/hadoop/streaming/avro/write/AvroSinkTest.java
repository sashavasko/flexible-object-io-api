package org.sv.flexobject.hadoop.streaming.avro.write;

import org.junit.Test;
import org.sv.flexobject.hadoop.streaming.avro.AvroSchema;
import org.sv.flexobject.hadoop.streaming.avro.read.AvroSource;
import org.sv.flexobject.hadoop.streaming.testdata.ObjectWithClass;
import org.sv.flexobject.hadoop.streaming.testdata.ObjectWithDate;
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

        System.out.println(testData);

        sink.put(testData);
        sink.close();

        AvroSource<TestDataWithSubSchemaInCollection> source = new AvroSource<>(TestDataWithSubSchemaInCollection.class);
        source.builder().forInput(os.toByteArray());

        TestDataWithSubSchemaInCollection result = source.get();

        assertEquals(testData, result);
    }

    @Test
    public void writeReadWithDates() throws Exception {
        ObjectWithDate testData = ObjectWithDate.random();

        System.out.println(AvroSchema.forClass(ObjectWithDate.class));

        AvroSink<ObjectWithDate> sink = new AvroSink<>();
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        sink.builder().forOutput(os).withSchema(ObjectWithDate.class);

        sink.put(testData);
        sink.close();

        AvroSource<ObjectWithDate> source = new AvroSource<>(ObjectWithDate.class);
        source.builder().forInput(os.toByteArray());

        ObjectWithDate result = source.get();

        assertEquals(testData.dateField.toLocalDate().toEpochDay(), result.dateField.toLocalDate().toEpochDay());
        assertEquals(testData.localDateField.toEpochDay(), result.localDateField.toEpochDay());
        assertEquals(testData.timestampField, result.timestampField);
    }

    @Test
    public void writeReadWithClass() throws Exception {
        ObjectWithClass testData = ObjectWithClass.random();

        System.out.println(AvroSchema.forClass(ObjectWithClass.class));

        AvroSink<ObjectWithClass> sink = new AvroSink<>();
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        sink.builder().forOutput(os).withSchema(ObjectWithClass.class);

        sink.put(testData);
        sink.close();

        AvroSource<ObjectWithClass> source = new AvroSource<>(ObjectWithClass.class);
        source.builder().forInput(os.toByteArray());

        ObjectWithClass result = source.get();

        assertEquals(testData, result);
        // org.sv.flexobject.hadoop.streaming.testdata.ObjectWithClass<{"classField":"org.sv.flexobject.hadoop.streaming.testdata.ObjectWithDate","classArray":[null,"org.sv.flexobject.hadoop.streaming.testdata.ObjectWithClass",null,null,null],"classList":["org.sv.flexobject.hadoop.streaming.testdata.ObjectWithClass"],"classMap":{"foo":"org.sv.flexobject.hadoop.streaming.testdata.ObjectWithClass"}}> but was:
        // org.sv.flexobject.hadoop.streaming.testdata.ObjectWithClass<{"classField":"org.sv.flexobject.hadoop.streaming.testdata.ObjectWithDate","classArray":[null,"org.sv.flexobject.hadoop.streaming.testdata.ObjectWithClass",null,null,null],"classList":["org.sv.flexobject.hadoop.streaming.testdata.ObjectWithClass"],"classMap":{"foo":"org.sv.flexobject.hadoop.streaming.testdata.ObjectWithClass"}}>
        //	at org.junit.Assert.fail(Assert.java:89)
    }

}