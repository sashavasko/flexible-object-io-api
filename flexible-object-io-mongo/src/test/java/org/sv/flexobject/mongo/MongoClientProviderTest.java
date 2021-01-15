package org.sv.flexobject.mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.backend.memory.MemoryBackend;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class MongoClientProviderTest {

    MongoClientProvider provider = new MongoClientProvider();

    @Test
    public void passwordClassName() {
        char[] secret = "blah".toCharArray();
        assertEquals(char[].class, secret.getClass());
    }


    MongoCollection<Document> collection;
    MongoClient client;
    MongoServer server;
    InetSocketAddress serverAddress;

    @Before
    public void setUp() {
        server = new MongoServer(new MemoryBackend());

        // optionally:
        // server.enableSsl(key, keyPassword, certificate);
        // server.enableOplog();

        // bind on a random local port
        serverAddress = server.bind();
        System.out.println("Started embedded Fake Mongo instance on " + serverAddress);
    }

    @After
    public void tearDown() {
        server.shutdown();
    }

    @Test
    public void testSimpleInsertQuery() throws Exception {

        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("url", "mongodb:/" + serverAddress.toString());

        try(MongoClient client = (MongoClient) provider.getConnection("blah", connectionProperties, null)){
            collection = client.getDatabase("testdb").getCollection("testcollection");

            assertEquals(0, collection.countDocuments());

            // creates the database and collection in memory and insert the object
            Document obj = new Document("_id", 1).append("key", "value");
            collection.insertOne(obj);

            assertEquals(1, collection.countDocuments());
            assertEquals(obj, collection.find().first());
        }
    }

}