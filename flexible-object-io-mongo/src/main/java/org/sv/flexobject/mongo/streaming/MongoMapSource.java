package org.sv.flexobject.mongo.streaming;

import com.mongodb.client.MongoCursor;

import java.util.Map;
import java.util.NoSuchElementException;

public class MongoMapSource<T> extends MongoCursorSource<T,Map<String, Object>> {
    public MongoMapSource() {
    }

    public MongoMapSource(MongoCursor cursor) {
        super(cursor);
    }

    @Override
    public T get() throws Exception {
        try {
            return (T) getCursor().next();
        }catch (NoSuchElementException e){
            return null;
        }
    }
}
