package org.sv.flexobject.mongo.streaming;

import org.sv.flexobject.mongo.connection.MongoConnection;

public class MongoConnectionOwner implements AutoCloseable{
    MongoConnection connection;

    public void setConnection(MongoConnection connection) {
        this.connection = connection;
    }

    public MongoConnection getConnection() {
        return connection;
    }

    @Override
    public void close(){
        if (connection != null){
            connection.close();
            connection = null;
        }
   }
}
