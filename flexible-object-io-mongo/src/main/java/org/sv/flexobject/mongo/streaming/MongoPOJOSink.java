package org.sv.flexobject.mongo.streaming;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.InsertOneResult;
import org.bson.BsonValue;
import org.bson.Document;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.mongo.schema.BsonSchema;
import org.sv.flexobject.stream.Sink;

public class MongoPOJOSink<POJO> extends MongoAbstractSink <POJO> {
    protected MongoCollection<POJO> collection;

    public MongoPOJOSink forCollection(MongoCollection<POJO> collection){
        this.collection = collection;
        return this;
    }

    @Override
    public boolean put(POJO value) throws Exception {
        return handleResult(collection.insertOne(value));
    }
}
