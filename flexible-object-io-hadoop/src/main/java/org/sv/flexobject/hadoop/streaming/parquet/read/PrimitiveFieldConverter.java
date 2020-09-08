package org.sv.flexobject.hadoop.streaming.parquet.read;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.schema.FieldDescriptor;

public class PrimitiveFieldConverter extends PrimitiveConverter {

    StreamableWithSchema current;

    FieldDescriptor fieldDescriptor;

    public PrimitiveFieldConverter(FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    private void set(Object value) {
        try {
            current.set(fieldDescriptor.getName(), value);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void addBinary(Binary value) {
        set(JsonNodeFactory.instance.binaryNode(value.getBytes()));
    }

    @Override
    public void addInt(int value) {
        set(value);
    }

    @Override
    public void addLong(long value) {
        set(value);
    }

    @Override
    public void addBoolean(boolean value) {
        set(value);
    }

    @Override
    public void addDouble(double value) {
        set(value);
    }

    @Override
    public void addFloat(float value) {
        set(value);
    }

    public void setCurrent (StreamableWithSchema group){
        current = group;
    }
}
