package org.sv.flexobject.mongo.streaming;

import com.mongodb.client.MongoCursor;

import java.util.Map;

public class MongoMapSource<T> extends MongoCursorSource<T,Map<String, Object>> {
    public MongoMapSource() {
    }

    public MongoMapSource(MongoCursor cursor) {
        super(cursor);
    }

    @Override
    public T get() throws Exception {
        return (T) getCursor().next();
    }
}
