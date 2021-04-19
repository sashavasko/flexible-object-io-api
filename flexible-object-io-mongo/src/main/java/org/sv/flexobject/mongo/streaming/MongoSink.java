package org.sv.flexobject.mongo.streaming;

import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.mongo.schema.BsonSchema;

public class MongoSink<SELF extends MongoSink> extends MongoAbstractSink<StreamableWithSchema> {

    protected MongoCollection<Document> collection;
    protected BsonSchema bsonSchema;

    public MongoSink() {
    }

    public MongoSink(Class<? extends StreamableWithSchema> schema) {
        forSchema(schema);
    }

    public SELF forCollection(MongoCollection<Document> collection){
        this.collection = collection;
        return (SELF) this;
    }

    public SELF forSchema(Class<? extends StreamableWithSchema> schema){
        bsonSchema = BsonSchema.getRegisteredSchema(schema);
        return (SELF) this;
    }

    @Override
    public boolean put(StreamableWithSchema value) throws Exception {
        if (bsonSchema == null)
            bsonSchema = BsonSchema.getRegisteredSchema(value.getClass());
        Document document = bsonSchema.toBson(value);
        return handleResult(collection.insertOne(document));
    }
}
