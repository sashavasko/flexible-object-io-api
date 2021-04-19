package org.sv.flexobject.mongo.streaming;

import com.mongodb.client.result.InsertOneResult;
import org.bson.BsonValue;
import org.sv.flexobject.stream.Sink;

public abstract class MongoAbstractSink<T> implements Sink<T> {
    protected BsonValue lastInsertId;

    public boolean handleResult(InsertOneResult result) throws Exception {
        lastInsertId = result.getInsertedId();
        return result.wasAcknowledged();
    }

    @Override
    public boolean hasOutput() {
        return lastInsertId != null;
    }

    public BsonValue getLastInsertId() {
        return lastInsertId;
    }
}
