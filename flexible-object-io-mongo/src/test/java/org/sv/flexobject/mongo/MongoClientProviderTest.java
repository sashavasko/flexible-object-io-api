package org.sv.flexobject.mongo;

import org.bson.Document;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MongoClientProviderTest extends EmbeddedMongoTest{

    MongoClientProvider provider = new MongoClientProvider();

    @Test
    public void passwordClassName() {
        char[] secret = "blah".toCharArray();
        assertEquals(char[].class, secret.getClass());
    }

    @Test
    public void testSimpleInsertQuery() throws Exception {
        assertEquals(0, collection.countDocuments());

        // creates the database and collection in memory and insert the object
        Document obj = new Document("_id", 1).append("key", "value");
        collection.insertOne(obj);

        assertEquals(1, collection.countDocuments());
        assertEquals(obj, collection.find().first());
    }
}