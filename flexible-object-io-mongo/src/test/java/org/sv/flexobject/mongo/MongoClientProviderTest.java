package org.sv.flexobject.mongo;

import com.mongodb.client.MongoClient;
import org.bson.Document;
import org.junit.Test;

import java.util.Properties;

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

    @Test
    public void usesHostsProperty() throws Exception {
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("url", "mongodb://foobar");
        connectionProperties.setProperty("hosts", serverAddress.toString().substring(1));

        client = (MongoClient) provider.getConnection("blah", connectionProperties, null);
        db = client.getDatabase("testdb");
        collection = db.getCollection(COLLECTION_NAME);

        assertEquals(0, collection.countDocuments());
    }
}