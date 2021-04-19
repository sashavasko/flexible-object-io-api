package org.sv.flexobject.mongo.streaming;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import org.bson.RawBsonDocument;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.mongo.schema.BsonSchema;
import org.sv.flexobject.stream.Source;

import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class MongoSource implements Source<StreamableWithSchema>,  Iterator<StreamableWithSchema>, Iterable<StreamableWithSchema>, AutoCloseable {
    MongoCursor<RawBsonDocument> cursor;
    Class<? extends StreamableWithSchema> schema;
    BsonSchema bsonSchema;

    public MongoSource() {
    }

    public MongoSource(Class<? extends StreamableWithSchema> schema) {
        forSchema(schema);
    }

    public MongoSource(MongoCursor<RawBsonDocument> cursor) {
        this.cursor = cursor;
    }

    public MongoSource forCursor(MongoCursor<RawBsonDocument> cursor){
        close();
        this.cursor = cursor;
        return this;
    }

    public MongoSource forCursor(FindIterable<RawBsonDocument> iterable){
        close();
        this.cursor = iterable.cursor();
        return this;
    }

    public MongoSource forSchema(Class<? extends StreamableWithSchema> schema){
        this.schema = schema;
        bsonSchema = BsonSchema.getRegisteredSchema(schema);
        return this;
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
