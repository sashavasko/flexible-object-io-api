package org.sv.flexobject.mongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import de.bwaldvogel.mongo.MongoServer;
import de.bwaldvogel.mongo.backend.memory.MemoryBackend;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;

import java.net.InetSocketAddress;
import java.util.Properties;

public class EmbeddedMongoTest {
    protected MongoServer server;
    protected InetSocketAddress serverAddress;
    protected MongoClientProvider provider = new MongoClientProvider();
    protected MongoClient client;
    protected MongoCollection<Document> collection;

    @Before
    public void setUp() throws Exception {
        server = new MongoServer(new MemoryBackend());

        // optionally:
        // server.enableSsl(key, keyPassword, certificate);
        // server.enableOplog();

        // bind on a random local port
        serverAddress = server.bind();
        System.out.println("Started embedded Fake Mongo instance on " + serverAddress);
        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("url", "mongodb:/" + serverAddress.toString());

        client = (MongoClient) provider.getConnection("blah", connectionProperties, null);
        collection = client.getDatabase("testdb").getCollection("testcollection");
    }

    @After
    public void tearDown() {
        client.close();
        server.shutdown();
    }


}
