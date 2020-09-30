package org.sv.flexobject.hadoop.streaming.parquet.write.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.sv.flexobject.hadoop.streaming.parquet.ParquetSchema;
import org.sv.flexobject.hadoop.streaming.parquet.ParquetSinkConf;
import org.sv.flexobject.hadoop.streaming.parquet.ParquetSourceConf;
import org.sv.flexobject.hadoop.streaming.parquet.write.SchemedWriteSupport;

import java.util.HashMap;
import java.util.Map;

public class JsonWriteSupport extends SchemedWriteSupport<JsonNode> {
    private ObjectNodeWriter groupWriter;

    public JsonWriteSupport(MessageType schema) {
        super(schema);
    }

    public JsonWriteSupport(MessageType schema, Map<String, String> extraMetaData) {
        super(schema, extraMetaData);
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        groupWriter = new ObjectNodeWriter(recordConsumer, schema);
    }

    @Override
    public void write(JsonNode jsonNode) {
        try {
            groupWriter.write((ObjectNode) jsonNode);
        } catch (JsonParquetException e) {
            throw new RuntimeException(e);
        }
    }
}
