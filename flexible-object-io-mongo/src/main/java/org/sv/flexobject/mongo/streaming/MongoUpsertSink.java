package org.sv.flexobject.mongo.streaming;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.sv.flexobject.Streamable;

public class MongoUpsertSink<SELF extends MongoUpsertSink> extends MongoSink<SELF> {

    boolean replaceDocuments = false;

    public Bson buildFilter(Streamable document){
        if (document instanceof MongoUpsertable){
            return ((MongoUpsertable) document).buildFilter();
        }
        throw new UnsupportedOperationException("Mongo Upserts are not supported for Class " + document.getClass() + ". Please implement MongoUpsertable.");
    }
    public Bson generateUpdate(Streamable document){
        if (document instanceof MongoUpsertable){
            return ((MongoUpsertable) document).generateUpdate();
        }
        throw new UnsupportedOperationException("Mongo Upserts are not supported for Class " + document.getClass() + ". Please implement MongoUpsertable.");
    }

    public MongoUpsertSink() {
    }

    public MongoUpsertSink(Class<? extends Streamable> schema) {
        super(schema);
    }

    public SELF replaceDocuments(boolean replaceDocuments){
        this.replaceDocuments = replaceDocuments;
        return (SELF)this;
    }

    @Override
    public boolean put(Streamable value) throws Exception {
        Bson filter = buildFilter(value);

        UpdateResult result;
        if (replaceDocuments) {
            ReplaceOptions options = new ReplaceOptions().upsert(true);
            Document document = getBsonSchema(value).toBson(value);
            result = getCollection().replaceOne(filter, document, options);
        } else {
            Bson document = generateUpdate(value);
            UpdateOptions options = new UpdateOptions().upsert(true);
            result = getCollection().updateOne(filter, document, options);
        }
        return handleResult(result);
    }

}
