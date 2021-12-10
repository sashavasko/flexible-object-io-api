package org.sv.flexobject.mongo.streaming;

import com.mongodb.client.MongoCursor;
import org.bson.RawBsonDocument;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.mongo.schema.BsonSchema;
import org.sv.flexobject.util.InstanceFactory;

import java.util.NoSuchElementException;

public class MongoSource extends MongoCursorSource<Streamable,RawBsonDocument> {
    Class<? extends Streamable> schema;
    BsonSchema bsonSchema;

    public MongoSource() {
    }

    public MongoSource(Class<? extends Streamable> schema, MongoCursor<RawBsonDocument> cursor) {
        super(cursor);
        this.schema = schema;
        bsonSchema = BsonSchema.getRegisteredSchema(schema);
    }

    public static class Builder extends MongoBuilder<Builder, MongoSource>{

        @Override
        public MongoSource build() throws Exception {
            MongoSource source = InstanceFactory.get(MongoSource.class);
            source.setCursor(getCursor(RawBsonDocument.class));
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
    public <T extends Streamable> T get() throws Exception {
        try {
            RawBsonDocument bson = cursor.next();
            Streamable record = bsonSchema.fromBson(bson);
            return (T) schema.cast(record);
        }catch (NoSuchElementException e){
            return null;
        }
    }

    @Override
    public Streamable next() {
        try {
            return get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
