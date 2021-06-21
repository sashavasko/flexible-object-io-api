package org.sv.flexobject.mongo.streaming;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import org.bson.RawBsonDocument;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.mongo.schema.BsonSchema;
import org.sv.flexobject.stream.Source;
import org.sv.flexobject.util.InstanceFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class MongoSource extends MongoConnectionOwner implements Source<StreamableWithSchema>,  Iterator<StreamableWithSchema>, Iterable<StreamableWithSchema>, AutoCloseable {
    MongoCursor<RawBsonDocument> cursor;
    Class<? extends StreamableWithSchema> schema;
    BsonSchema bsonSchema;

    public MongoSource() {
    }

    public MongoSource(Class<? extends StreamableWithSchema> schema, MongoCursor<RawBsonDocument> cursor) {
        this.cursor = cursor;
        this.schema = schema;
        bsonSchema = BsonSchema.getRegisteredSchema(schema);
    }

    public static class Builder extends MongoBuilder<Builder>{

        public MongoSource build() throws Exception {
            MongoSource source = InstanceFactory.get(MongoSource.class);
            source.cursor = getCursor(RawBsonDocument.class);
            source.schema = getSchema();
            source.bsonSchema = getBsonSchema();
            saveConnection(source);
            return source;
        }
    }

    public static Builder builder() {
        return InstanceFactory.get(Builder.class);
    }

    @Override
    public <T extends StreamableWithSchema> T get() throws Exception {
        return (T) schema.cast(bsonSchema.fromBson(cursor.next()));
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
    public StreamableWithSchema next() {
        try {
            return get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
