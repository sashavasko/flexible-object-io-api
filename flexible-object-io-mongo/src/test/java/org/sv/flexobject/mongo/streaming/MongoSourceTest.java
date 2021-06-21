package org.sv.flexobject.mongo.streaming;

import com.mongodb.client.model.Filters;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.sv.flexobject.mongo.EmbeddedMongoTest;
import org.sv.flexobject.mongo.schema.BsonSchema;
import org.sv.flexobject.mongo.schema.testdata.ObjectWithObjectId;
import org.sv.flexobject.testdata.TestDataWithSubSchema;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class MongoSourceTest extends EmbeddedMongoTest {

    @Test
    public void testObjectWithId() throws Exception {
        ObjectWithObjectId data = ObjectWithObjectId.random();
        collection.insertOne(BsonSchema.serialize(data));

        try(MongoSource source = new MongoSource(ObjectWithObjectId.class,
                collectionRaw.find(Filters.eq("_id", new ObjectId(data.objectId))).cursor())) {

            assertEquals(data, source.get());
        }
    }

    @Test
    public void testObjectWithSubSchemaSeveral() throws Exception {
        List<TestDataWithSubSchema> listOfData = new ArrayList<>();
        for (int i = 0 ; i < 10 ; ++i) {
            TestDataWithSubSchema data = TestDataWithSubSchema.random(true);
            listOfData.add(data);
            collection.insertOne(BsonSchema.serialize(data));
        }

        List<TestDataWithSubSchema> listOfConvertedData = new ArrayList<>();
        try(MongoSource source = new MongoSource(TestDataWithSubSchema.class, collectionRaw.find().cursor())) {

            while (source.hasNext())
                listOfConvertedData.add(source.get());
        }

        assertEquals(10, listOfConvertedData.size());
        assertTrue(listOfData.containsAll(listOfConvertedData));
    }
}