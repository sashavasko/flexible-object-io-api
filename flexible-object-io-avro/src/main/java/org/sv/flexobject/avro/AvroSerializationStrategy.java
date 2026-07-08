package org.sv.flexobject.avro;

import org.sv.flexobject.Streamable;
import org.sv.flexobject.serde.SerializationStrategy;

import java.io.IOException;

public class AvroSerializationStrategy implements SerializationStrategy {

    public static final SerializationStrategy AVRO = new AvroSerializationStrategy();

    private AvroSerializationStrategy(){}

    @Override
    public byte[] serialize(Streamable datum) throws IOException {
        return AvroSerializer.forData(datum).start().write(datum).asBytes();
    }

    @Override
    public void deserialize(Streamable datum, byte[] bytes, int offset, int length) throws IOException {
        AvroSerializer.forData(datum).startRead().readOne(bytes, offset, length, datum);
    }
}
