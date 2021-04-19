package org.sv.flexobject.mongo.streaming;

import com.mongodb.client.MongoCursor;
import org.sv.flexobject.stream.Source;

import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class MongoPOJOSource<POJO> implements Source<POJO>,  Iterator<POJO>, Iterable<POJO>, AutoCloseable {
    MongoCursor<POJO> cursor;

    public MongoPOJOSource() {
    }

    public MongoPOJOSource(MongoCursor<POJO> cursor) {
        this.cursor = cursor;
    }

    public MongoPOJOSource forCursor(MongoCursor<POJO> cursor){
        close();
        this.cursor = cursor;
        return this;
    }

    @Override
    public POJO get() throws Exception {
        return cursor.next();
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
    public POJO next() {
        return cursor.next();
    }
}
