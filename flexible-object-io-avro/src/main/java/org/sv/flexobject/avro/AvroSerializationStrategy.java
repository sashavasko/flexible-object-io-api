package org.sv.flexobject.avro;

import org.sv.flexobject.Streamable;
import org.sv.flexobject.serde.SerializationStrategy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class AvroSerializationStrategy implements SerializationStrategy {

    public static final SerializationStrategy AVRO = new AvroSerializationStrategy();
    public static final byte[] CONTENT_TYPE = "application/avro".getBytes(StandardCharsets.UTF_8);

    protected AvroSerializationStrategy(){}

    @Override
    public byte[] serialize(Streamable datum) throws IOException {
        return AvroSerializer.forData(datum).start().write(datum).asBytes();
    }

    @Override
    public void deserialize(Streamable datum, byte[] bytes, int offset, int length) throws IOException {
        AvroSerializer.forData(datum).startRead().readOne(bytes, offset, length, datum);
    }

    @Override
    public byte[] getContentType() {
        return CONTENT_TYPE;
    }
}
