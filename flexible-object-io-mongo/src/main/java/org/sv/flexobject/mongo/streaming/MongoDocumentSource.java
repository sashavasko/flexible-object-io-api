package org.sv.flexobject.mongo.streaming;

import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.sv.flexobject.util.InstanceFactory;

import java.util.Iterator;

public class MongoDocumentSource extends MongoMapSource {
    public MongoDocumentSource() {
    }

    public MongoDocumentSource(MongoCursor<Document> cursor) {
        super(cursor);
    }

    public static class Builder extends MongoBuilder<Builder>{

        public MongoDocumentSource build() throws Exception {
            MongoDocumentSource source = InstanceFactory.get(MongoDocumentSource.class);
            source.cursor = getCursor(Document.class);
            saveConnection(source);
            return source;
        }
    }

    public static Builder builder() {
        return InstanceFactory.get(Builder.class);
    }

    @Override
    public Document get() throws Exception {
        return (Document) super.get();
    }

//    @Override
//    public Iterator<Document> iterator() {
//        return cursor;
//    }

    @Override
    public Document next() {
        return (Document) super.next();
    }


}
