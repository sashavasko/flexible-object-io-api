package org.sv.flexobject.mongo.connection;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.sv.flexobject.connections.ConnectionManager;
import org.sv.flexobject.mongo.MongoClientProvider;
import org.sv.flexobject.util.InstanceFactory;

import java.util.Properties;

public class MongoConnection implements AutoCloseable{
    MongoClient client;
    MongoDatabase db;

    @Override
    public void close() {
        client.close();
    }

    public MongoClient getClient() {
        return client;
    }

    public MongoDatabase getDb() {
        return db;
    }

    public static class Builder<SELF extends Builder> {
        String connectionName;
        String dbName;
        Properties overrides = new Properties();
        Object secret;

        public SELF forName(String connectionName){
            this.connectionName = connectionName;
            return (SELF) this;
        }

        public SELF db(String dbName){
            this.dbName = dbName;
            return (SELF) this;
        }

        public SELF password(Object secret){
            this.secret = secret;
            return (SELF) this;
        }

        public SELF override(String propertyName, String propertyValue){
            overrides.setProperty(propertyName, propertyValue);
            return (SELF)this;
        }

        public SELF override(Properties overrides){
            this.overrides.putAll(overrides);
            return (SELF)this;
        }

        public MongoConnection build() throws Exception {
            MongoConnection connection = InstanceFactory.get(MongoConnection.class);

            if (StringUtils.isNotBlank(connectionName)){
                connection.client = (MongoClient) ConnectionManager.getConnection(MongoClient.class, connectionName, overrides);
            } else if (overrides != null){
                if (secret == null){
                    if (overrides.containsKey("secret"))
                        secret = overrides.getProperty("secret");
                    else if (overrides.containsKey("password"))
                        secret = overrides.getProperty("password");
                    else if (overrides.containsKey("pwd"))
                        secret = overrides.getProperty("pwd");
                }
                connection.client = (MongoClient) MongoClientProvider.getConnection(overrides, secret);
            } else
                throw new RuntimeException("Can't create Mongo Connection: Either connection name or url in properties must be set");
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
