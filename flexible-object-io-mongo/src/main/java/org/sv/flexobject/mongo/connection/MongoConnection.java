package org.sv.flexobject.mongo.connection;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.sv.flexobject.connections.ConnectionManager;
import org.sv.flexobject.util.InstanceFactory;

import java.util.Properties;

public class MongoConnection implements AutoCloseable{
    MongoClient client;
    MongoDatabase db;

    @Override
    public void close() {
        client.close();
    }

    public static class Builder<SELF extends Builder> {
        String connectionName;
        String dbName;
        Properties overrides = new Properties();

        public SELF forName(String connectionName){
            this.connectionName = connectionName;
            return (SELF) this;
        }

        public SELF db(String dbName){
            this.dbName = dbName;
            return (SELF) this;
        }

        public SELF override(String propertyName, String propertyValue){
            overrides.setProperty(propertyName, propertyValue);
            return (SELF)this;
        }

        public SELF override(Properties overrides){
            overrides.putAll(overrides);
            return (SELF)this;
        }

        public MongoConnection build() throws Exception {
            MongoConnection connection = InstanceFactory.get(MongoConnection.class);

            connection.client = (MongoClient) ConnectionManager.getConnection(MongoClient.class, connectionName, overrides);
            connection.db = connection.client.getDatabase(dbName);
            return connection;
        }
    }

    public static Builder builder(){
        return InstanceFactory.get(Builder.class);
    }

    public MongoCollection<Document> getCollection(String collectionName){
        return db.getCollection(collectionName);
    }

    public MongoCollection<RawBsonDocument> getRawCollection(String collectionName){
        return db.getCollection(collectionName, RawBsonDocument.class);
    }

    /**
     * Gets a collection, with a specific default document class.
     *
     * @param collectionName the name of the collection to return
     * @param documentClass  the default class to cast any documents returned from the database into.
     * @param <TDocument>    the type of the class to use instead of {@code Document}.
     * @return the collection
     */
    public <TDocument> MongoCollection<TDocument> getCollection(String collectionName, Class<TDocument> documentClass){
        return db.getCollection(collectionName, documentClass);
    }

}
