package org.sv.flexobject.mongo.streaming;

import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.mongo.schema.BsonSchema;

public class MongoSink<SELF extends MongoSink> extends MongoAbstractSink<Streamable> {

    protected MongoCollection<Document> collection;
    protected BsonSchema bsonSchema;

    public MongoSink() {
    }

    public MongoSink(Class<? extends Streamable> schema) {
        forSchema(schema);
    }

    public SELF forCollection(MongoCollection<Document> collection){
        this.collection = collection;
        return (SELF) this;
    }

    public SELF forSchema(Class<? extends Streamable> schema){
        bsonSchema = BsonSchema.getRegisteredSchema(schema);
        return (SELF) this;
    }

    @Override
    public boolean put(Streamable value) throws Exception {
        if (bsonSchema == null)
            bsonSchema = BsonSchema.getRegisteredSchema(value.getClass());
        Document document = bsonSchema.toBson(value);
        return handleResult(collection.insertOne(document));
    }
}
