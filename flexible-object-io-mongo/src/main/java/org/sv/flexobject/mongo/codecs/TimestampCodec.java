package org.sv.flexobject.mongo.codecs;

import org.apache.logging.log4j.Logger;
import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.sv.flexobject.mongo.schema.BsonSchema;

import java.sql.Timestamp;

public class TimestampCodec implements Codec<Timestamp> {

    @Override
    public void encode(BsonWriter writer, Timestamp value, EncoderContext encoderContext) {
        try {
            writer.writeTimestamp(BsonSchema.toBsonTimestamp(value));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Class<Timestamp> getEncoderClass() {
        return Timestamp.class;
    }

    @Override
    public Timestamp decode(BsonReader reader, DecoderContext decoderContext) {
        try {
            return (Timestamp) BsonSchema.bsonTimestampConverter.apply(reader.readTimestamp());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
