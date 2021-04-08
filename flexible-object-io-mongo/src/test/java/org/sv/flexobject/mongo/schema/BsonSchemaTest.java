package org.sv.flexobject.mongo.schema;

import com.mongodb.client.MongoCollection;
import org.bson.*;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.sv.flexobject.mongo.EmbeddedMongoTest;
import org.sv.flexobject.mongo.schema.testdata.ObjectWithObjectId;
import org.sv.flexobject.mongo.schema.testdata.ObjectWithTimestampAndDate;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.testdata.levelone.leveltwo.SimpleObject;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.Date;

import static org.junit.Assert.*;

public class BsonSchemaTest extends EmbeddedMongoTest {

    BsonSchema bsonSchema;

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
        assertTrue(document.get("objectId") instanceof ObjectId);
        assertEquals(data.objectId, ((ObjectId) document.get("objectId")).toHexString());

        ObjectWithObjectId convertedData = bsonSchema.fromBson(document);
        assertEquals(data, convertedData);

        ObjectWithObjectId dataWithNullOid = ObjectWithObjectId.random();
        dataWithNullOid.set("objectId", null);
        Document documentWithNullOid = bsonSchema.toBson(dataWithNullOid);

        assertEquals(data.get("intField"), DataTypes.int32Converter(document.get("intField")));
        assertFalse(documentWithNullOid.containsKey("objectId"));

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
}