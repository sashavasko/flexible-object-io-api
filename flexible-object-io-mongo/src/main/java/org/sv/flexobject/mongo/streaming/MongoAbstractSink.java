package org.sv.flexobject.mongo.streaming;

import com.mongodb.bulk.BulkWriteInsert;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.InsertManyResult;
import com.mongodb.client.result.InsertOneResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.BsonValue;
import org.sv.flexobject.stream.Sink;

import java.util.HashMap;
import java.util.Map;

public abstract class MongoAbstractSink<T> implements Sink<T> {
    protected BsonValue lastInsertId;
    protected Map<Integer, BsonValue> insertIds;
    protected long lastModifiedCount;

    public boolean handleResult(BulkWriteResult result) throws Exception {
        int index = 0;
        if (insertIds == null)
            insertIds = new HashMap<>();
        else
            insertIds.clear();
        for (BulkWriteInsert insert : result.getInserts()) {
            insertIds.put(index++, insert.getId());
            lastInsertId = insert.getId();
        }
        lastModifiedCount = result.getModifiedCount();
        return result.wasAcknowledged();
    }

    public boolean handleResult(InsertOneResult result) throws Exception {
        lastInsertId = result.getInsertedId();
        lastModifiedCount = 1;
        return result.wasAcknowledged();
    }

    public boolean handleResult(UpdateResult result) throws Exception {
        lastInsertId = result.getUpsertedId();
        lastModifiedCount = result.getModifiedCount();
        return result.wasAcknowledged();
    }

    public boolean handleResult(DeleteResult result) throws Exception {
        lastModifiedCount = result.getDeletedCount();
        return result.wasAcknowledged();
    }

    public boolean handleResult(InsertManyResult result) throws Exception {
        insertIds = result.getInsertedIds();
        return result.wasAcknowledged();
    }

    @Override
    public boolean hasOutput() {
        return lastInsertId != null;
    }

    public BsonValue getLastInsertId() {
        return lastInsertId;
    }

    public long getLastModifiedCount() {
        return lastModifiedCount;
    }

    public Map<Integer, BsonValue> getInsertIds() {
        return insertIds;
    }
}
