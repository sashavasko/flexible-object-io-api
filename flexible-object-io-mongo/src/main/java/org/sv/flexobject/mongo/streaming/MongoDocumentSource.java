package org.sv.flexobject.mongo.streaming;

import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.sv.flexobject.util.InstanceFactory;

public class MongoDocumentSource extends MongoMapSource<Document> {
    public MongoDocumentSource() {
    }

    public MongoDocumentSource(MongoCursor<Document> cursor) {
        super(cursor);
    }

    public static class Builder extends MongoBuilder<Builder, MongoDocumentSource>{

        @Override
        public MongoDocumentSource build() throws Exception {
            MongoDocumentSource source = InstanceFactory.get(MongoDocumentSource.class);
            source.setCursor(getCursor(Document.class));
            saveConnection(source);
            return source;
        }
    }

    public static Builder builder() {
        return InstanceFactory.get(Builder.class);
    }


//    @Override
//    public Iterator<Document> iterator() {
//// TODO :
////        return cursor;
//        return null;
//    }
}
