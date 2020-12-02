package org.sv.flexobject.mongo.streaming;

import com.mongodb.client.MongoCursor;
import org.sv.flexobject.stream.Source;

import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class MongoMapSource<SELF extends MongoMapSource> implements Source<Map<String, Object>>,  Iterator<Map<String, Object>>, Iterable<Map<String, Object>>, AutoCloseable {
    MongoCursor cursor;

    public MongoMapSource() {
    }

    public MongoMapSource(MongoCursor cursor) {
        this.cursor = cursor;
    }

    public SELF forCursor(MongoCursor cursor){
        close();
        this.cursor = cursor;
        return (SELF) this;
    }

    @Override
    public Map<String, Object> get() throws Exception {
        return (Map<String, Object>) cursor.next();
    }

    @Override
    public boolean isEOF() {
        return cursor.hasNext();
    }

    @Override
    public void close() {
        if (cursor != null) {
            cursor.close();
            cursor = null;
        }
    }

    @Override
    public Stream stream() {
        return StreamSupport.stream(spliterator(), false);
    }

    @Override
    public Iterator iterator() {
        return cursor;
    }

    @Override
    public boolean hasNext() {
        return cursor.hasNext();
    }

    @Override
    public Map<String,Object> next() {
        return (Map<String, Object>) cursor.next();
    }


}
