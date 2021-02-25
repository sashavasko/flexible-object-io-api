package org.sv.flexobject.hadoop.streaming.avro.read;

import org.apache.avro.generic.GenericRecord;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.hadoop.streaming.avro.AvroSchema;

public class AvroSource<T extends StreamableWithSchema> extends AvroGenericSource<GenericRecord, T> {

    Class<? extends StreamableWithSchema> schema;

    public AvroSource(Class<? extends StreamableWithSchema> schema) {
        this.schema = schema;
        builder().withSchema(schema);
    }

    @Override
    protected T unwrap(GenericRecord value) {
        try {
            T convertedValue = (T) AvroSchema.convertGenericRecord(value, value.getSchema());
            return convertedValue;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
