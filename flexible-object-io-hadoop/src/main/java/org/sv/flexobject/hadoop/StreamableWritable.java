package org.sv.flexobject.hadoop;

import org.apache.hadoop.io.Writable;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.avro.AvroSerializationStrategy;
import org.sv.flexobject.avro.AvroSerializer;
import org.sv.flexobject.serde.SerializationStrategy;
import org.sv.flexobject.util.BiConsumerWithException;
import org.sv.flexobject.util.ByteRepresentable;
import org.sv.flexobject.util.FunctionWithException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface StreamableWritable extends Streamable, Writable, ByteRepresentable {
    default SerializationStrategy getStrategy() throws IOException {
        // Json format is way more expensive for a single record as it also stores field names in it
        return AvroSerializationStrategy.AVRO;
    }

    default byte[] toBytes() throws IOException {
        return getStrategy().serialize(this);
    }

    default void fromBytes(byte[] bytes){
        fromBytes(bytes, 0, bytes.length);
    }
    default void fromBytes(byte[] bytes, int offset, int length){
        try {
            getStrategy().deserialize(this, bytes, offset, length);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    default void write(DataOutput out) throws IOException {
        byte[] bytes = toBytes();
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    @Override
    default void readFields(DataInput in) throws IOException {
        int length = in.readInt();
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        fromBytes(bytes);
    }
}
