package org.sv.flexobject.hadoop.streaming.avro.read;

import org.apache.avro.generic.GenericRecord;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.hadoop.streaming.avro.AvroSchema;

public class AvroSource extends AvroGenericSource<GenericRecord, Streamable> {

    Class<? extends Streamable> schema;

    public AvroSource() {
    }

    public AvroSource(Class<? extends Streamable> schema) {
        forSchema(schema);
    }

    public AvroSource forSchema(Class<? extends Streamable> schema) {
        this.schema = schema;
        builder().withSchema(schema);
        return this;
    }

    @Override
    protected <T extends Streamable> T unwrap(GenericRecord value) {
        try {
            T convertedValue = (T) AvroSchema.convertGenericRecord(value, value.getSchema());
            return convertedValue;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
