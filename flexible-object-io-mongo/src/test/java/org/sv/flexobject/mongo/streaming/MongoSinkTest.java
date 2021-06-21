package org.sv.flexobject.mongo.streaming;

import com.mongodb.client.model.Filters;
import org.bson.BsonValue;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.sv.flexobject.mongo.EmbeddedMongoTest;
import org.sv.flexobject.mongo.schema.BsonSchema;
import org.sv.flexobject.mongo.schema.testdata.ObjectWithObjectId;
import org.sv.flexobject.testdata.TestDataWithSubSchema;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class MongoSinkTest extends EmbeddedMongoTest {

    @Test
    public void testObjectWithId() throws Exception {
        ObjectWithObjectId data = ObjectWithObjectId.random();

        MongoSink sink = new MongoSink(ObjectWithObjectId.class).forCollection(collection);

        sink.put(data);
        BsonValue id = sink.getLastInsertId();

        assertEquals(data.objectId, id.asObjectId().getValue().toHexString());

        ObjectWithObjectId deserializedData = BsonSchema.fromBson(collection.find().first(), ObjectWithObjectId.class);
        assertEquals(data, deserializedData);
    }

    @Test
    public void testObjectWithSubSchemaSeveral() throws Exception {
        List<TestDataWithSubSchema> listOfData = new ArrayList<>();
        List<BsonValue> insertedIds = new ArrayList<>();
        MongoSink sink = new MongoSink().forCollection(collection);
        for (int i = 0; i < 10; ++i) {
            TestDataWithSubSchema data = TestDataWithSubSchema.random(true);
            listOfData.add(data);

            assertTrue(sink.put(data));

            insertedIds.add(sink.getLastInsertId());
        }

        assertEquals(10, insertedIds.size());

        List<TestDataWithSubSchema> listOfConvertedData = new ArrayList<>();
        try (MongoSource source = new MongoSource(TestDataWithSubSchema.class, collectionRaw.find().cursor())) {

            while (source.hasNext())
                listOfConvertedData.add(source.get());
        }

        assertEquals(10, listOfConvertedData.size());
        assertTrue(listOfData.containsAll(listOfConvertedData));
    }
}