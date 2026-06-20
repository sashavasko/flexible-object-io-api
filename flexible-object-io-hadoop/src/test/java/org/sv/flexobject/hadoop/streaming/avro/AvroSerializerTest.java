package org.sv.flexobject.hadoop.streaming.avro;

import org.junit.jupiter.api.Test;
import org.sv.flexobject.json.MapperFactory;
import org.sv.flexobject.testdata.TestDataWithEnumAndClass;
import org.sv.flexobject.testdata.TestDataWithSubSchema;
import org.sv.flexobject.testdata.TestDataWithSubSchemaInCollection;
import org.sv.flexobject.testdata.levelone.ObjectWithNestedObject;
import org.sv.flexobject.testdata.levelone.leveltwo.SimpleObject;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class AvroSerializerTest {

    @Test
    void simpleObject() throws Exception {
        SimpleObject<?> recordIn = new SimpleObject<>();
        recordIn.intField = 5;

        byte[] bytes = AvroSerializer.toBytes(AvroSerializer.toBytes(recordIn));

        assertEquals(14, recordIn.toJsonBytes().length);
        assertEquals(2, bytes.length);

        SimpleObject<?> recordOut = AvroSerializer.fromBytes(bytes, SimpleObject.class);

        assertEquals(recordIn, recordOut);
    }

    @Test
    void simpleObjectRandom() throws Exception {
        SimpleObject<?> recordIn = new SimpleObject<>();
        recordIn.intField = 158052356;

        byte[] bytes = AvroSerializer.toBytes(AvroSerializer.toBytes(recordIn));

        assertEquals(22, recordIn.toJsonBytes().length);
        assertEquals(6, bytes.length);

        SimpleObject<?> recordOut = AvroSerializer.fromBytes(bytes, SimpleObject.class);
        assertEquals(recordIn, recordOut);
    }

    @Test
    void objectWithNestedObject() throws Exception {
        ObjectWithNestedObject recordIn = ObjectWithNestedObject.random();

        byte[] bytes = AvroSerializer.toBytes(AvroSerializer.toBytes(recordIn));

        System.out.println("JSON bytes length:" + recordIn.toJsonBytes().length);
        System.out.println("AVRO bytes length:" + bytes.length);

        ObjectWithNestedObject recordOut = AvroSerializer.fromBytes(bytes, ObjectWithNestedObject.class);
        assertEquals(recordIn, recordOut);
    }

    @Test
    void objectWithNestedObjectInMap() throws Exception {
        ObjectWithNestedObject recordIn = ObjectWithNestedObject.random();

        byte[] bytes = AvroSerializer.toBytes(AvroSerializer.toBytes(recordIn));

        System.out.println("JSON bytes length:" + recordIn.toJsonBytes().length);
        System.out.println("AVRO bytes length:" + bytes.length);

        ObjectWithNestedObject recordOut = AvroSerializer.fromBytes(bytes, ObjectWithNestedObject.class);
        assertEquals(recordIn, recordOut);
    }

    @Test
    void fooBar() throws Exception {
        FooBar recordIn = FooBar.random(false);

        byte[] bytes = AvroSerializer.toBytes(AvroSerializer.toBytes(recordIn));

        System.out.println("JSON bytes length:" + recordIn.toJsonBytes().length);
        System.out.println("AVRO bytes length:" + bytes.length);

        FooBar recordOut = AvroSerializer.fromBytes(bytes, FooBar.class);
        assertEquals(recordIn, recordOut);
    }

    @Test
    void subSchema() throws Exception {
        TestDataWithSubSchema recordIn = TestDataWithSubSchema.random(false);

        System.out.println(MapperFactory.pretty(recordIn.toJson()));

        byte[] bytes = AvroSerializer.toBytes(AvroSerializer.toBytes(recordIn));
        System.out.println(Arrays.toString(bytes));

        System.out.println("JSON bytes length:" + recordIn.toJsonBytes().length);
        System.out.println("AVRO bytes length:" + bytes.length);

        TestDataWithSubSchema recordOut = AvroSerializer.fromBytes(bytes, TestDataWithSubSchema.class);
        assertEquals(recordIn, recordOut);
    }

    @Test
    void subSchemaInCollection() throws Exception {
        TestDataWithSubSchemaInCollection recordIn = TestDataWithSubSchemaInCollection.random(true);

        byte[] bytes = AvroSerializer.toBytes(AvroSerializer.toBytes(recordIn));

        System.out.println("JSON bytes length:" + recordIn.toJsonBytes().length);
        System.out.println("AVRO bytes length:" + bytes.length);

        TestDataWithSubSchemaInCollection recordOut = AvroSerializer.fromBytes(bytes, TestDataWithSubSchemaInCollection.class);
        assertEquals(recordIn, recordOut);
    }

    @Test
    void enumAndClass() throws Exception {
        TestDataWithEnumAndClass recordIn = TestDataWithEnumAndClass.random();

        byte[] bytes = AvroSerializer.toBytes(AvroSerializer.toBytes(recordIn));

        System.out.println("JSON bytes length:" + recordIn.toJsonBytes().length);
        System.out.println("AVRO bytes length:" + bytes.length);

        TestDataWithEnumAndClass recordOut = AvroSerializer.fromBytes(bytes, TestDataWithEnumAndClass.class);
        assertEquals(recordIn, recordOut);
    }
}