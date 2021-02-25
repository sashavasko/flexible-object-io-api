package org.sv.flexobject.hadoop.streaming.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.schema.FieldDescriptor;
import org.sv.flexobject.schema.SchemaException;

public class StreamableAvroRecord implements GenericRecord {

    StreamableWithSchema wrappedObject;
    Schema schema;

    public StreamableAvroRecord() {
    }

    public StreamableAvroRecord(StreamableWithSchema wrappedObject, Schema avroSchema) {
        set(wrappedObject, avroSchema);
    }

    public void set(StreamableWithSchema wrappedObject, Schema avroSchema) {
        this.wrappedObject = wrappedObject;
        schema = avroSchema;
    }

    @Override
    public void put(String key, Object v) {
        try {
            wrappedObject.set(key, v);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object get(String key) {
        try {
            return wrappedObject.get(key);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void put(int i, Object v) {
        try {
            org.sv.flexobject.schema.Schema wrappedSchema = wrappedObject.getSchema();
            FieldDescriptor descriptor = wrappedSchema.getDescriptor(i);
            if (v instanceof Utf8)
                descriptor.set(wrappedObject, v.toString());
            else
                descriptor.set(wrappedObject, v);
        } catch (SchemaException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object get(int i) {
        try {
            org.sv.flexobject.schema.Schema internalSchema = wrappedObject.getSchema();
            if (internalSchema == null)
                throw new SchemaException("Missing schema for class " + wrappedObject.getClass());
            FieldDescriptor descriptor = internalSchema.getDescriptor(i);
            Object value = descriptor.get(wrappedObject);

            return AvroSchema.toAvro(descriptor, value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    @Override
    public String toString() {
        return "StreamableAvroRecord{" +
                "wrappedObject=" + wrappedObject +
                ", schema=" + schema +
                '}';
    }
}
