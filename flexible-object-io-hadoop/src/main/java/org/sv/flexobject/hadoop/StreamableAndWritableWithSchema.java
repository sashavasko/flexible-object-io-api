package org.sv.flexobject.hadoop;

import org.apache.hadoop.io.Writable;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.schema.SchemaElement;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StreamableAndWritableWithSchema<T extends SchemaElement> extends StreamableWithSchema<T> implements Writable {

    @Override
    public void write(DataOutput out) throws IOException {
        try {
            byte[] bytes = toJsonBytes();
            out.writeInt(bytes.length);
            out.write(bytes);
        } catch (Exception e) {
            throw new IOException("Failed to serialize object of class " + getClass().getName(), e);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int length = in.readInt();
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        try {
            fromJsonBytes(bytes);
        } catch (Exception e) {
            throw new IOException("Failed to de-serialize object of class " + getClass().getName(), e);
        }
    }
}
