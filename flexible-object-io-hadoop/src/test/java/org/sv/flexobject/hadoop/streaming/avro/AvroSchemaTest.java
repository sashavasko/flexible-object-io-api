package org.sv.flexobject.hadoop.streaming.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;
import org.sv.flexobject.testdata.ObjectWithNestedObjectWithNestedObject;
import org.sv.flexobject.testdata.TestDataWithSubSchemaInCollection;
import org.sv.flexobject.testdata.levelone.ObjectWithNestedObject;
import org.sv.flexobject.testdata.levelone.ObjectWithNestedObjectInMap;
import org.sv.flexobject.testdata.levelone.leveltwo.SimpleObject;

import static org.junit.Assert.assertEquals;

public class AvroSchemaTest {

    @Test
    public void nestedObjects() {
        assertEquals("{\"type\":\"record\",\"name\":\"SimpleObject\",\"namespace\":\"org.sv.flexobject.testdata.levelone.leveltwo\",\"fields\":[{\"name\":\"intField\",\"type\":[\"null\",\"int\"]}]}"
                , AvroSchema.forClass(SimpleObject.class).toString());
        Schema avro = AvroSchema.forClass(ObjectWithNestedObject.class);
        assertEquals("{\"type\":\"record\",\"name\":\"ObjectWithNestedObject\",\"namespace\":\"org.sv.flexobject.testdata.levelone\",\"fields\":[{\"name\":\"intField\",\"type\":[\"null\",\"int\"]},{\"name\":\"nestedObject\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"SimpleObject\",\"namespace\":\"org.sv.flexobject.testdata.levelone.leveltwo\",\"fields\":[{\"name\":\"intField\",\"type\":[\"null\",\"int\"]}]}]}]}"
                , avro.toString());
        assertEquals("{\"type\":\"record\",\"name\":\"ObjectWithNestedObjectWithNestedObject\",\"namespace\":\"org.sv.flexobject.testdata\",\"fields\":[{\"name\":\"intField\",\"type\":[\"null\",\"int\"]},{\"name\":\"objectWithNestedObject\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"ObjectWithNestedObject\",\"namespace\":\"org.sv.flexobject.testdata.levelone\",\"fields\":[{\"name\":\"intField\",\"type\":[\"null\",\"int\"]},{\"name\":\"nestedObject\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"SimpleObject\",\"namespace\":\"org.sv.flexobject.testdata.levelone.leveltwo\",\"fields\":[{\"name\":\"intField\",\"type\":[\"null\",\"int\"]}]}]}]}]}]}"
                , AvroSchema.forClass(ObjectWithNestedObjectWithNestedObject.class).toString());
    }

    @Test
    public void nestedObjectsInMap() {
        assertEquals("{\"type\":\"record\",\"name\":\"ObjectWithNestedObjectInMap\",\"namespace\":\"org.sv.flexobject.testdata.levelone\",\"fields\":[{\"name\":\"intField\",\"type\":[\"null\",\"int\"]},{\"name\":\"subStructMap\",\"type\":[\"null\",{\"type\":\"map\",\"values\":[\"null\",{\"type\":\"record\",\"name\":\"SimpleObject\",\"namespace\":\"org.sv.flexobject.testdata.levelone.leveltwo\",\"fields\":[{\"name\":\"intField\",\"type\":[\"null\",\"int\"]}]}]}]}]}"
                , AvroSchema.forClass(ObjectWithNestedObjectInMap.class).toString());
    }

    @Test
    public void fullSimple() throws Exception {
        ObjectWithNestedObject testData = ObjectWithNestedObject.random();
        Schema avroSchema = AvroSchema.forClass(ObjectWithNestedObject.class);

        GenericRecord avro = AvroOutputAdapter.produce(avroSchema, testData::save);
        ObjectWithNestedObject testDataBack = new ObjectWithNestedObject();
        AvroInputAdapter.consume(avroSchema, avro, testDataBack::load);

        assertEquals(testData, testDataBack);
    }

    @Test
    public void forClass() throws Exception {
        Schema avroSchema = AvroSchema.forClass(TestDataWithSubSchemaInCollection.class);

        assertEquals("{\"type\":\"record\",\"name\":\"TestDataWithSubSchemaInCollection\",\"namespace\":\"org.sv.flexobject.testdata\",\"fields\":[{\"name\":\"intField\",\"type\":[\"null\",\"int\"]},{\"name\":\"intFieldOptional\",\"type\":[\"null\",\"int\"]},{\"name\":\"json\",\"type\":[\"null\",\"string\"]},{\"name\":\"subStructArray\",\"type\":[\"null\",{\"type\":\"array\",\"items\":[\"null\",{\"type\":\"record\",\"name\":\"TestDataWithInferredSchema\",\"fields\":[{\"name\":\"intField\",\"type\":[\"null\",\"int\"]},{\"name\":\"intFieldOptional\",\"type\":[\"null\",\"int\"]},{\"name\":\"intFieldStoredAsString\",\"type\":[\"null\",\"string\"]},{\"name\":\"intArray\",\"type\":[\"null\",{\"type\":\"array\",\"items\":[\"null\",\"int\"]}]},{\"name\":\"intList\",\"type\":[\"null\",{\"type\":\"array\",\"items\":[\"null\",\"int\"]}]},{\"name\":\"intMap\",\"type\":[\"null\",{\"type\":\"map\",\"values\":[\"null\",\"int\"]}]},{\"name\":\"json\",\"type\":[\"null\",\"string\"]}]}]}]},{\"name\":\"subStructList\",\"type\":[\"null\",{\"type\":\"array\",\"items\":[\"null\",\"TestDataWithInferredSchema\"]}]},{\"name\":\"subStructMap\",\"type\":[\"null\",{\"type\":\"map\",\"values\":[\"null\",\"TestDataWithInferredSchema\"]}]}]}"
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