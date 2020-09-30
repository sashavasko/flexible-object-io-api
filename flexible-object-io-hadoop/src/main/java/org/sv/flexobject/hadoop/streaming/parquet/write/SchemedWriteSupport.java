package org.sv.flexobject.hadoop.streaming.parquet.write;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.sv.flexobject.hadoop.streaming.parquet.write.json.JsonParquetException;
import org.sv.flexobject.hadoop.streaming.parquet.write.json.ObjectNodeWriter;

import java.util.HashMap;
import java.util.Map;

public abstract class SchemedWriteSupport<T> extends WriteSupport<T> {
    private Map<String, String> extraMetaData;
    protected MessageType schema;

    public SchemedWriteSupport(MessageType schema) {
        extraMetaData = new HashMap<>();
        this.schema = schema;
    }

    public SchemedWriteSupport(MessageType schema, Map<String, String> extraMetaData) {
        this.extraMetaData = extraMetaData;
        this.schema = schema;
    }

    @Override
    public WriteContext init(Configuration configuration) {
        if(schema == null){
            throw new RuntimeException("Unknown schema for parquet Output.");
        }
        return new WriteContext(schema, this.extraMetaData);
    }
}
