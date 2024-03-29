package org.sv.flexobject.hadoop.streaming.parquet.write.streamable;

import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.hadoop.streaming.parquet.write.SchemedWriteSupport;

import java.util.Map;

public class ParquetWriteSupport extends SchemedWriteSupport<Streamable, StreamableParquetWriter> {

    public ParquetWriteSupport() {
    }

    public ParquetWriteSupport(MessageType schema) {
        super(schema);
    }

    public ParquetWriteSupport(MessageType schema, Map<String, String> extraMetaData) {
        super(schema, extraMetaData);
    }

    @Override
    protected StreamableParquetWriter createWriter(RecordConsumer recordConsumer) {
        return new StreamableParquetWriter(recordConsumer, getSchema());
    }
}
