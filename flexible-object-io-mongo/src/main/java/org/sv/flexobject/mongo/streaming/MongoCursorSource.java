package org.sv.flexobject.mongo.streaming;

import com.mongodb.client.MongoCursor;
import org.sv.flexobject.stream.Source;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public abstract class MongoCursorSource<T,BSON> extends MongoConnectionOwner implements Source<T>,  Iterator<T>, Iterable<T>, AutoCloseable {

    MongoCursor<BSON> cursor;

    public MongoCursorSource() {
    }

    public MongoCursorSource(MongoCursor cursor) {
        this.cursor = cursor;
    }

    public MongoCursor<BSON> getCursor() {
        return cursor;
    }

    public void setCursor(MongoCursor cursor) {
        this.cursor = cursor;
    }

    @Override
    public boolean isEOF() {
        return !cursor.hasNext();
    }

    @Override
    public void close() {
        if (cursor != null) {
            cursor.close();
            cursor = null;
        }
        super.close();
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
    public T next() {
        try {
            return (T) cursor.next();
        }catch (NoSuchElementException e){
            return null;
        }
    }
}
