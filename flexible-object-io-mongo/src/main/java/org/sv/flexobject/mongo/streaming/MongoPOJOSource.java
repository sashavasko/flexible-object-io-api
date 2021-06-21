package org.sv.flexobject.mongo.streaming;

import com.mongodb.client.MongoCursor;
import org.sv.flexobject.stream.Source;
import org.sv.flexobject.util.InstanceFactory;

import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class MongoPOJOSource<POJO>  extends MongoConnectionOwner implements Source<POJO>,  Iterator<POJO>, Iterable<POJO>, AutoCloseable {
    MongoCursor<POJO> cursor;

    public MongoPOJOSource() {
    }

    public MongoPOJOSource(MongoCursor<POJO> cursor) {
        this.cursor = cursor;
    }

    public static class Builder extends MongoBuilder<Builder>{

        public MongoPOJOSource build() throws Exception {
            MongoPOJOSource source = InstanceFactory.get(MongoPOJOSource.class);
            source.cursor = getCursor(getDocumentClass());
            saveConnection(source);
            return source;
        }
    }

    public static Builder builder() {
        return InstanceFactory.get(Builder.class);
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
    public POJO next() {
        return cursor.next();
    }
}
