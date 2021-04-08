package org.sv.flexobject.mongo.codecs;

import org.bson.BsonReader;
import org.bson.BsonWriter;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.sv.flexobject.mongo.schema.BsonSchema;

import java.sql.Date;

public class SqlDateCodec implements Codec<Date> {

    @Override
    public void encode(BsonWriter writer, Date value, EncoderContext encoderContext) {
        try {
            writer.writeDateTime(BsonSchema.toBsonDateTime(value).getValue());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Class<Date> getEncoderClass() {
        return Date.class;
    }

    @Override
    public Date decode(BsonReader reader, DecoderContext decoderContext) {
        try {
            return new Date(reader.readDateTime());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
