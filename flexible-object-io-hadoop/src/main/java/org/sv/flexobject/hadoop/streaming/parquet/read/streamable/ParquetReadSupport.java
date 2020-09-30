package org.sv.flexobject.hadoop.streaming.parquet.read.streamable;

import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.schema.MessageType;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.hadoop.streaming.parquet.read.SchemedGroupConverter;
import org.sv.flexobject.hadoop.streaming.parquet.read.SchemedReadSupport;

public class ParquetReadSupport extends SchemedReadSupport<StreamableWithSchema> {

    public ParquetReadSupport(MessageType schema) {
        super(schema);
    }

    @Override
    public SchemedGroupConverter<StreamableWithSchema> newGroupConverter(MessageType schema) {
        return new StreamableConverter(schema);
    }
}
