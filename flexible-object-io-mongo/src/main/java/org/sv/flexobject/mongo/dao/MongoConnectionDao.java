package org.sv.flexobject.mongo.dao;

import com.mongodb.client.MongoCollection;
import org.sv.flexobject.mongo.connection.MongoConnection;
import org.sv.flexobject.util.AutoCloseables;

import java.io.PrintStream;

public class MongoConnectionDao<SELF extends MongoConnectionDao> implements AutoCloseable {
    String connectionName;
    String dbName;
    MongoConnection connection;
    static PrintStream trace = null;

    public static void enableTrace() {
        enableTrace(System.out);
    }
    public static void enableTrace(PrintStream stream) {
        trace = stream;
    }

    public static void disableTrace() {
        trace = null;
    }

    private void printTrace(String message){
        if (trace != null) {
            trace.println(message);
            Thread thread = Thread.currentThread();
            StackTraceElement[] stackTrace = thread.getStackTrace();
            for (StackTraceElement stackTraceElement : stackTrace) {
                if (stackTraceElement.getClassName().startsWith("org.sv.flexobject"))
                    trace.println("\t" + stackTraceElement);
            }
        }
    }

    public MongoConnection getConnection() {
        if (connection == null){
            synchronized (this) {
                if (connection == null) {
                    MongoConnection.Builder builder = MongoConnection.builder()
                            .forName(connectionName)
                            .db(dbName);
                    try {
                        connection = builder.build();
                        printTrace("Created connection for " + connectionName + " and database " + dbName);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        return connection;
    }

    public MongoCollection getCollection(String collectionName) {
        return getConnection().getCollection(collectionName);
    }

    public SELF forConnectionName(String connectionName) {
        this.connectionName = connectionName;
        AutoCloseables.closeQuietly(this);
        return (SELF) this;
    }

    public void setConnectionName(String connectionName) {
        forConnectionName(connectionName);
    }

    public String getConnectionName() {
        return connectionName;
    }

    public String getDbName() {
        return dbName;
    }

    public SELF setDbName(String dbName) {
        this.dbName = dbName;
        AutoCloseables.closeQuietly(this);
        return (SELF) this;
    }

    public void setConnection(MongoConnection connection) {
        this.connection = connection;
    }

    @Override
    public void close() throws Exception {
        if (connection!= null){
            connection.close();
            printTrace ("Closed connection for " + connectionName + " and database " + dbName);
            connection = null;
        }
    }
}
