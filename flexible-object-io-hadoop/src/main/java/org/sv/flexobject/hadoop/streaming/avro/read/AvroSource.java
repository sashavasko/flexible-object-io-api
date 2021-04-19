package org.sv.flexobject.hadoop.streaming.avro.read;

import org.apache.avro.generic.GenericRecord;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.hadoop.streaming.avro.AvroSchema;

public class AvroSource extends AvroGenericSource<GenericRecord, StreamableWithSchema> {

    Class<? extends StreamableWithSchema> schema;

    public AvroSource() {
    }

    public AvroSource(Class<? extends StreamableWithSchema> schema) {
        forSchema(schema);
    }

    public AvroSource forSchema(Class<? extends StreamableWithSchema> schema) {
        this.schema = schema;
        builder().withSchema(schema);
        return this;
    }

    @Override
    protected <T extends StreamableWithSchema> T unwrap(GenericRecord value) {
        try {
            T convertedValue = (T) AvroSchema.convertGenericRecord(value, value.getSchema());
            return convertedValue;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
