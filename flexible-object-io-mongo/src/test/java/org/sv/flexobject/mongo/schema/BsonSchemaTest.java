package org.sv.flexobject.mongo.schema;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.client.MongoCollection;
import org.bson.*;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.json.MapperFactory;
import org.sv.flexobject.mongo.EmbeddedMongoTest;
import org.sv.flexobject.mongo.schema.testdata.ObjectWithArrayOfStrings;
import org.sv.flexobject.mongo.schema.testdata.ObjectWithObjectId;
import org.sv.flexobject.mongo.schema.testdata.ObjectWithSetOfLongs;
import org.sv.flexobject.mongo.schema.testdata.ObjectWithTimestampAndDate;
import org.sv.flexobject.schema.AbstractFieldDescriptor;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.schema.SchemaElement;
import org.sv.flexobject.testdata.TestDataWithInferredSchema;
import org.sv.flexobject.testdata.TestDataWithSubSchema;
import org.sv.flexobject.testdata.levelone.leveltwo.SimpleObject;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;

import static org.junit.Assert.*;

public class BsonSchemaTest extends EmbeddedMongoTest {

    BsonSchema bsonSchema;

    @Test
    public void testSetOfLongs() throws Exception {
        ObjectWithSetOfLongs data = new ObjectWithSetOfLongs();
        data.getSchema();
        long val1 = 178575;
        long val2 = 178675;
        long val3 = 178375;
        data.add(val1);
        data.add(val2);
        data.add(val3);
        data.add(ObjectWithSetOfLongs.TestEnum.one);
        data.add(ObjectWithSetOfLongs.TestEnum.two);

        bsonSchema = BsonSchema.getRegisteredSchema(data.getClass());
        collection.insertOne(bsonSchema.toBson(data));

        assertEquals(1, collection.countDocuments());
        Document documentInCollection = collection.find().first();

        ObjectWithSetOfLongs dataOut = bsonSchema.fromBson(documentInCollection);
        assertEquals(data, dataOut);
    }

    @Test
    public void testEmptySet() throws Exception {
        ObjectWithSetOfLongs data = new ObjectWithSetOfLongs();
        data.getSchema();
        data.add(ObjectWithSetOfLongs.TestEnum.one);
        data.add(ObjectWithSetOfLongs.TestEnum.two);

        bsonSchema = BsonSchema.getRegisteredSchema(data.getClass());
        collection.insertOne(bsonSchema.toBson(data));

        assertEquals(1, collection.countDocuments());
        Document documentInCollection = collection.find().first();

        assertFalse(documentInCollection.containsKey("set"));
        ObjectWithSetOfLongs dataOut = bsonSchema.fromBson(documentInCollection);
        assertEquals(data, dataOut);
    }

    @Test
    public void testEmptyEnumSet() throws Exception {
        ObjectWithSetOfLongs data = new ObjectWithSetOfLongs();
        data.getSchema();
        long val1 = 178575;
        long val2 = 178675;
        long val3 = 178375;
        data.add(val1);
        data.add(val2);
        data.add(val3);

        bsonSchema = BsonSchema.getRegisteredSchema(data.getClass());
        collection.insertOne(bsonSchema.toBson(data));

        assertEquals(1, collection.countDocuments());
        Document documentInCollection = collection.find().first();

        assertFalse(documentInCollection.containsKey("enumSet"));

        ObjectWithSetOfLongs dataOut = bsonSchema.fromBson(documentInCollection);
        assertEquals(data, dataOut);
    }

    @Test
    public void testSimpleInsertQuery() throws Exception {
        SimpleObject data = SimpleObject.random();
        bsonSchema = BsonSchema.getRegisteredSchema(data.getClass());
        collection.insertOne(bsonSchema.toBson(data));

        assertEquals(1, collection.countDocuments());
        Document documentInCollection = collection.find().first();
        assertEquals(data.get("intField"), documentInCollection.get("intField"));
    }


    @Test
    public void toBson() throws Exception {
        SimpleObject data = new SimpleObject();
        BsonSchema bsonSchema = BsonSchema.getRegisteredSchema(data.getClass());
        Document document = bsonSchema.toBson(data);

        assertEquals(0, (int)DataTypes.int32Converter(document.get("intField")));

        data = SimpleObject.random();

        document = bsonSchema.toBson(data);
        assertEquals(data.get("intField"), DataTypes.int32Converter(document.get("intField")));

        SimpleObject convertedData = bsonSchema.fromBson(document);
        assertEquals(data, convertedData);

        collection.insertOne(document);

        assertEquals(1, collection.countDocuments());
        Document documentInCollection = collection.find().first();
        assertEquals(data.get("intField"), documentInCollection.get("intField"));

        convertedData = bsonSchema.fromBson(documentInCollection);
        assertEquals(data, convertedData);

        RawBsonDocument rawDocumentInCollection = collectionRaw.find().first();

        BsonValue bsonValue = rawDocumentInCollection.get("intField");
        assertEquals(BsonType.INT32, bsonValue.getBsonType());
        assertEquals(data.intField, bsonValue.asInt32().getValue());

        convertedData = bsonSchema.fromBson(rawDocumentInCollection);
        assertEquals(data, convertedData);

        MongoCollection<SimpleObject> pojoCollection = db.getCollection(COLLECTION_NAME, SimpleObject.class);

        convertedData = pojoCollection.find().first();
        assertEquals(data, convertedData);
    }

    @Test
    public void toBsonWithObjectId() throws Exception {
        ObjectWithObjectId data = ObjectWithObjectId.random();
        BsonSchema bsonSchema = BsonSchema.getRegisteredSchema(data.getClass());
        Document document = bsonSchema.toBson(data);

        assertEquals(data.get("intField"), DataTypes.int32Converter(document.get("intField")));
        assertTrue(document.get("_id") instanceof ObjectId);
        assertEquals(data.objectId, ((ObjectId) document.get("_id")).toHexString());

        ObjectWithObjectId convertedData = bsonSchema.fromBson(document);
        assertEquals(data, convertedData);

        ObjectWithObjectId dataWithNullOid = ObjectWithObjectId.random();
        dataWithNullOid.set("objectId", null);
        Document documentWithNullOid = bsonSchema.toBson(dataWithNullOid);

        assertEquals(data.get("intField"), DataTypes.int32Converter(document.get("intField")));
        assertFalse(documentWithNullOid.containsKey("_id"));

        convertedData = bsonSchema.fromBson(documentWithNullOid);
        assertEquals(dataWithNullOid, convertedData);

        collection.insertOne(document);

        assertEquals(1, collection.countDocuments());
        Document documentInCollection = collection.find().first();

        convertedData = bsonSchema.fromBson(documentInCollection);
        assertEquals(data, convertedData);

        RawBsonDocument rawDocumentInCollection = collectionRaw.find().first();

        convertedData = bsonSchema.fromBson(rawDocumentInCollection);
        assertEquals(data, convertedData);

        MongoCollection<ObjectWithObjectId> pojoCollection = db.getCollection(COLLECTION_NAME, ObjectWithObjectId.class);

        convertedData = pojoCollection.find().first();
        assertEquals(data, convertedData);
    }

    @Test
    public void toBsonWithDatesAndTimestamps() throws Exception {
        ObjectWithTimestampAndDate data = new ObjectWithTimestampAndDate();
        BsonSchema bsonSchema = BsonSchema.getRegisteredSchema(data.getClass());
        Document document = bsonSchema.toBson(data);

        assertEquals(1, document.size());

        data = ObjectWithTimestampAndDate.random();
        document = bsonSchema.toBson(data);

        assertEquals(data.get("intField"), DataTypes.int32Converter(document.get("intField")));

        long time = ((BsonTimestamp)document.get("timestamp")).getTime();
        long millis = ((BsonTimestamp)document.get("timestamp")).getInc()/1000000;
        assertEquals(data.timestamp.getTime(), time * 1000l + millis);
        assertEquals(data.timestamp, DataTypes.timestampConverter(document.get("timestamp")));

        Timestamp actualTimestamp = DataTypes.timestampConverter(document.get("timestampFromDate"));
        assertEquals(data.timestampFromDate.getTime(), actualTimestamp.getTime());
        Date actualDate = DataTypes.dateConverter(document.get("timestampFromDate"));
        assertEquals(data.timestampFromDate, actualDate);

        actualTimestamp = DataTypes.timestampConverter(document.get("dateFromTimestamp"));
        assertEquals(data.dateFromTimestamp, actualTimestamp);

        actualDate = DataTypes.dateConverter(document.get("date"));
        assertEquals(data.date, actualDate);
        LocalDate actualLocalDate = DataTypes.localDateConverter(document.get("localDate"));
        assertEquals(data.localDate, actualLocalDate);

        ObjectWithTimestampAndDate convertedData = bsonSchema.fromBson(document);
        assertEquals(data, convertedData);

        collection.insertOne(document);

        assertEquals(1, collection.countDocuments());
        Document documentInCollection = collection.find().first();

        convertedData = bsonSchema.fromBson(documentInCollection);
        assertEquals(data, convertedData);

        RawBsonDocument rawDocumentInCollection = collectionRaw.find().first();

        convertedData = bsonSchema.fromBson(rawDocumentInCollection);
        assertEquals(data, convertedData);

        MongoCollection<ObjectWithTimestampAndDate> pojoCollection = db.getCollection(COLLECTION_NAME, ObjectWithTimestampAndDate.class);

        convertedData = pojoCollection.find().first();

        // These cannot be done using standard mongo pojo codecs :
        data.dateFromTimestamp = null;
        data.timestampFromDate = null;
        assertEquals(data, convertedData);
    }

    public void toBsonTestDataWithInferredSchema() throws Exception {
        TestDataWithInferredSchema data = new TestDataWithInferredSchema();
        BsonSchema bsonSchema = BsonSchema.getRegisteredSchema(data.getClass());
        Document document = bsonSchema.toBson(data);

        assertEquals(6, document.size());

        data = TestDataWithInferredSchema.random(true);
        document = bsonSchema.toBson(data);

        TestDataWithInferredSchema convertedData = bsonSchema.fromBson(document);
        assertEquals(data, convertedData);

        collection.insertOne(document);

        assertEquals(1, collection.countDocuments());
        Document documentInCollection = collection.find().first();

        convertedData = bsonSchema.fromBson(documentInCollection);
        assertEquals(data, convertedData);

        RawBsonDocument rawDocumentInCollection = collectionRaw.find().first();

        convertedData = bsonSchema.fromBson(rawDocumentInCollection);
        assertEquals(data, convertedData);

        // Unsupported for POJOs with arrays!
//        MongoCollection<TestDataWithInferredSchema> pojoCollection = db.getCollection(COLLECTION_NAME, TestDataWithInferredSchema.class);
//        convertedData = pojoCollection.find().first();
//        assertEquals(data, convertedData);
    }

    @Test
    public void toBsonTestDataWithSubSchema() throws Exception {
        TestDataWithSubSchema data = new TestDataWithSubSchema();
        BsonSchema bsonSchema = BsonSchema.getRegisteredSchema(data.getClass());
        Document document = bsonSchema.toBson(data);

        assertEquals(2, document.size());

        data = TestDataWithSubSchema.random(true);
        data.json = (ObjectNode) MapperFactory.getObjectReader().readTree("{\"foo\":\"one\",\"bar\":\"yes\"}");
        document = bsonSchema.toBson(data);

        TestDataWithSubSchema convertedData = bsonSchema.fromBson(document);
        assertEquals(data, convertedData);

        collection.insertOne(document);

        assertEquals(1, collection.countDocuments());
        Document documentInCollection = collection.find().first();

        convertedData = bsonSchema.fromBson(documentInCollection);
        assertEquals(data, convertedData);

        RawBsonDocument rawDocumentInCollection = collectionRaw.find().first();

        convertedData = bsonSchema.fromBson(rawDocumentInCollection);
        assertEquals(data, convertedData);

        // Unsupported for POJOs with arrays!
//        MongoCollection<TestDataWithInferredSchema> pojoCollection = db.getCollection(COLLECTION_NAME, TestDataWithInferredSchema.class);
//        convertedData = pojoCollection.find().first();
//        assertEquals(data, convertedData);
    }

    public static boolean compareFields(Object o1, Object o2){
        for (SchemaElement field : ((Streamable)o1).getSchema().getFields()) {
            AbstractFieldDescriptor descriptor = field.getDescriptor();
            Object value = descriptor.get(o1);
            Object otherValue = descriptor.get(o2);
            if (value != null) {
                if (otherValue == null){
                    throw new RuntimeException("o1:" + value + " o2:" + otherValue + " " + field);
                }

                if (value.getClass().isArray() != otherValue.getClass().isArray()){
                    throw new RuntimeException("o1:" + value + " o2:" + otherValue + " " + field);
                }

                if (value.getClass().isArray()) {
                    if (!Arrays.equals((Object[])value, (Object[])otherValue)) {
                        throw new RuntimeException("o1:" + value + " o2:" + otherValue + " " + field);
                    }
                } else if (value instanceof Map) {
                    if (otherValue instanceof Map){
                        return ((Map)value).entrySet().equals(((Map)otherValue).entrySet());
                    } else{
                        throw new RuntimeException("o1:" + value + " o2:" + otherValue + " " + field);
                    }
                } else {
                    if (value instanceof Timestamp && otherValue instanceof Timestamp){
                        Timestamp t1 = (Timestamp) value;
                        Timestamp t2 = (Timestamp) otherValue;
                        if (t1.getTime() != t2.getTime() || t1.getNanos() != t2.getNanos()) {
                            throw new RuntimeException("o1.time:" + t1.getTime() + " o2.time:" + t2.getTime() + "o1.nanos:" + t1.getNanos() + " o2.nanos:" + t2.getNanos() + " t1:" + t1 + " t2:" + t2);
                        } else {
                            if (!t1.equals(t2))
                                throw new RuntimeException("Timestamp comparison sucks!");
                            return true;
                        }
                    } else if (!value.equals(otherValue)) {
                        throw new RuntimeException("o1:" + value + " o2:" + otherValue + " " + field);
                    }
                }
            } else if (otherValue != null){
                throw new RuntimeException("o1:" + value + " o2:" + otherValue + " " + field);
            }
        }

        return true;
    }

    @Test
    public void toFromBsonWithEmptyArray() throws Exception {
        ObjectWithArrayOfStrings data = new ObjectWithArrayOfStrings();
        BsonSchema bsonSchema = BsonSchema.getRegisteredSchema(data.getClass());
        Document bson = bsonSchema.toBson(data);

        ObjectWithArrayOfStrings convertedData = bsonSchema.fromBson(bson);
        assertEquals(data, convertedData);
    }

    @Test
    public void toFromBsonWithArrayValues() throws Exception {
        ObjectWithArrayOfStrings data = new ObjectWithArrayOfStrings();
        data.array[1] = "foo";
        data.array[3] = "bar";

        BsonSchema bsonSchema = BsonSchema.getRegisteredSchema(data.getClass());
        Document bson = bsonSchema.toBson(data);

        ObjectWithArrayOfStrings convertedData = bsonSchema.fromBson(bson);
        assertEquals(data, convertedData);
    }
}