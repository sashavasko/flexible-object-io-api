package org.sv.flexobject.mongo.dao;

import org.junit.jupiter.api.Test;
import org.sv.flexobject.mongo.EmbeddedMongoTest;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.junit.jupiter.api.Assertions.*;

public class MongoConnectionDaoTest extends EmbeddedMongoTest {

    @Test
    public void getConnectionWithTrace() throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        MongoConnectionDao.enableTrace(ps);
        try(MongoConnectionDao dao = new MongoConnectionDao<>()) {
            dao.setConnectionName("test");
            dao.setDbName(dbName);
            dao.getConnection();
            dao.getConnection();
            dao.getConnection();
        }
        String expectedOutput = "Created connection for test and database testdb\n" +
                "\torg.sv.flexobject.mongo.dao.MongoConnectionDao.printTrace(MongoConnectionDao.java:30)\n" +
                "\torg.sv.flexobject.mongo.dao.MongoConnectionDao.getConnection(MongoConnectionDao.java:47)\n" +
                "\torg.sv.flexobject.mongo.dao.MongoConnectionDaoTest.getConnectionWithTrace(MongoConnectionDaoTest.java:21)\n" +
                "Closed connection for test and database testdb\n" +
                "\torg.sv.flexobject.mongo.dao.MongoConnectionDao.printTrace(MongoConnectionDao.java:30)\n" +
                "\torg.sv.flexobject.mongo.dao.MongoConnectionDao.close(MongoConnectionDao.java:93)\n" +
                "\torg.sv.flexobject.mongo.dao.MongoConnectionDaoTest.getConnectionWithTrace(MongoConnectionDaoTest.java:24)\n";
        assertEquals(expectedOutput, baos.toString());
    }
}