package org.sv.flexobject.mongo.streaming;

import com.mongodb.client.MongoCursor;
import org.bson.Document;

import java.util.Iterator;

public class MongoDocumentSource extends MongoMapSource<MongoDocumentSource> {
    public MongoDocumentSource() {
    }

    public MongoDocumentSource(MongoCursor<Document> cursor) {
        super(cursor);
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
