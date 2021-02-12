package org.sv.flexobject.hadoop.streaming.parquet.write;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.sv.flexobject.hadoop.streaming.parquet.ParquetSchemaConf;

import java.util.HashMap;
import java.util.Map;

public abstract class SchemedWriteSupport<T,WT extends SchemedWriter> extends WriteSupport<T> {
    private Map<String, String> extraMetaData;
    private MessageType schema;
    private WT groupWriter;

    public SchemedWriteSupport() {
        extraMetaData = new HashMap<>();
    }

    public SchemedWriteSupport(MessageType schema) {
        this();
        this.schema = schema;
    }

    public SchemedWriteSupport(MessageType schema, Map<String, String> extraMetaData) {
        this.extraMetaData = extraMetaData;
        this.schema = schema;
    }

    @Override
    public WriteContext init(Configuration configuration) {
        if (schema == null) {
            ParquetSchemaConf conf = new ParquetSchemaConf().from(configuration);
            schema = conf.getOutputSchema();
            if (schema == null)
                throw new RuntimeException("Unknown Parquet Output Schema");
        }
        return new WriteContext(schema, this.extraMetaData);
    }

    public MessageType getSchema() {
        return schema;
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        groupWriter = createWriter(recordConsumer);
    }

    @Override
    public void write(T group) {
        try {
            groupWriter.write(group);
        } catch (ParquetWriteException e) {
            throw new RuntimeException(e);
        }
    }

    abstract protected WT createWriter(RecordConsumer recordConsumer);

}
