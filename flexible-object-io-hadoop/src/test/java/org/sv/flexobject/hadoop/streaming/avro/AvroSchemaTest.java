package org.sv.flexobject.hadoop.streaming.avro;

import com.carfax.hadoop.streaming.TestDataWithSubSchemaInCollection;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AvroSchemaTest {

    @Test
    public void forClass() throws Exception {
        Schema avroSchema = AvroSchema.forClass(TestDataWithSubSchemaInCollection.class);

        assertEquals("{\"type\":\"record\",\"name\":\"TestDataWithSubSchemaInCollection\",\"namespace\":\"com.carfax.hadoop.streaming\",\"fields\":[{\"name\":\"intField\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"intFieldOptional\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"json\",\"type\":[\"null\",\"bytes\"],\"default\":null},{\"name\":\"subStructArray\",\"type\":[\"null\",{\"type\":\"array\",\"items\":[\"null\",{\"type\":\"record\",\"name\":\"element\",\"namespace\":\"\",\"fields\":[{\"name\":\"intField\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"intFieldOptional\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"intFieldStoredAsString\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"intArray\",\"type\":[\"null\",{\"type\":\"array\",\"items\":[\"null\",\"int\"]}],\"default\":null},{\"name\":\"intList\",\"type\":[\"null\",{\"type\":\"array\",\"items\":[\"null\",\"int\"]}],\"default\":null},{\"name\":\"intMap\",\"type\":[\"null\",{\"type\":\"map\",\"values\":[\"null\",\"int\"]}],\"default\":null},{\"name\":\"json\",\"type\":[\"null\",\"bytes\"],\"default\":null}]}]}],\"default\":null},{\"name\":\"subStructList\",\"type\":[\"null\",{\"type\":\"array\",\"items\":[\"null\",\"element\"]}],\"default\":null},{\"name\":\"subStructMap\",\"type\":[\"null\",{\"type\":\"map\",\"values\":[\"null\",{\"type\":\"record\",\"name\":\"value\",\"namespace\":\"\",\"fields\":[{\"name\":\"intField\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"intFieldOptional\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"intFieldStoredAsString\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"intArray\",\"type\":[\"null\",{\"type\":\"array\",\"items\":[\"null\",\"int\"]}],\"default\":null},{\"name\":\"intList\",\"type\":[\"null\",{\"type\":\"array\",\"items\":[\"null\",\"int\"]}],\"default\":null},{\"name\":\"intMap\",\"type\":[\"null\",{\"type\":\"map\",\"values\":[\"null\",\"int\"]}],\"default\":null},{\"name\":\"json\",\"type\":[\"null\",\"bytes\"],\"default\":null}]}]}],\"default\":null}]}"
                , avroSchema.toString());
    }

    @Test
    public void fullCircle() throws Exception {
        TestDataWithSubSchemaInCollection testData = TestDataWithSubSchemaInCollection.random(true);
        Schema avroSchema = AvroSchema.forClass(TestDataWithSubSchemaInCollection.class);

        GenericRecord avro = AvroOutputAdapter.produce(avroSchema, testData::save);
        TestDataWithSubSchemaInCollection testDataBack = new TestDataWithSubSchemaInCollection();
        AvroInputAdapter.consume(avroSchema, avro, testDataBack::load);

        assertEquals(testData, testDataBack);
    }
}