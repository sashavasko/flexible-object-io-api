package org.sv.flexobject.mongo.streaming;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.*;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.sv.flexobject.Streamable;

import java.util.ArrayList;
import java.util.List;

public class MongoBulkUpsertSink<SELF extends MongoBulkUpsertSink> extends MongoUpsertSink<SELF> {

    int batchSize = 1000;
    List<WriteModel<Document>> pendingWrites = new ArrayList<>();

    public SELF setBatchSize(int batchSize) {
        this.batchSize = batchSize;
        return (SELF) this;
    }

    @Override
    public boolean put(Streamable value) throws Exception {
        writePending(false);
        Bson filter = buildFilter(value);

        if (replaceDocuments) {
            ReplaceOptions options = new ReplaceOptions().upsert(true);
            Document document = getBsonSchema(value).toBson(value);
            pendingWrites.add(new ReplaceOneModel<>(filter, document, options));
        } else {
            Bson document = generateUpdate(value);
            UpdateOptions options = new UpdateOptions().upsert(true);
            pendingWrites.add(new UpdateOneModel<>(filter, document, options));
        }
        return true;
    }

    private boolean writePending(boolean force) throws Exception {
        if (pendingWrites.isEmpty()) return false;
        if (!force && pendingWrites.size() < batchSize) return false;
        BulkWriteResult result = getCollection().bulkWrite(pendingWrites);
        pendingWrites.clear();
        return handleResult(result);
    }

    @Override
    public boolean hasOutput() {
        return super.hasOutput() || !pendingWrites.isEmpty();
    }

    @Override
    public void setEOF() {
        try {
            writePending(true);
        } catch (Exception e) {
            throw new RuntimeException("Failed to flush pending writes to Mongo", e);
        }
        super.setEOF();
    }
}
