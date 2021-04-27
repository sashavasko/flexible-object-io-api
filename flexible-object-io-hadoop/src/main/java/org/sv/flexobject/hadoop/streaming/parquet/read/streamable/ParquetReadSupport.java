package org.sv.flexobject.hadoop.streaming.parquet.read.streamable;

import org.apache.parquet.schema.MessageType;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.hadoop.streaming.parquet.ParquetSchema;
import org.sv.flexobject.hadoop.streaming.parquet.read.SchemedGroupConverter;
import org.sv.flexobject.hadoop.streaming.parquet.read.SchemedReadSupport;

public class ParquetReadSupport extends SchemedReadSupport<StreamableWithSchema> {

    public ParquetReadSupport() {
    }

    public ParquetReadSupport(MessageType schema) {
        super(schema);
    }

    @Override
    public SchemedGroupConverter<StreamableWithSchema> newGroupConverter(MessageType schema, MessageType fileSchema) {
        return new StreamableConverter(schema, fileSchema, ParquetSchema.forType(schema));
    }
}
