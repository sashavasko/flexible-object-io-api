package org.sv.flexobject.hadoop;

import org.sv.flexobject.Streamable;
import org.sv.flexobject.io.Serializer;
import org.sv.flexobject.json.JsonSerializer;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface StreamableWritable extends Streamable, Writable {

    default Serializer getStrategy() throws IOException {
        // Avro format is way more expensive for a single record as it also stores schema in it
        return JsonSerializer.instance;
    }

    default byte[] toBytes() throws IOException {
        return getStrategy().ser(this);
    }

    default void fromBytes(byte[] data) throws IOException {
        getStrategy().deser(this, data);
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
