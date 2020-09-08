package org.sv.flexobject.hadoop.streaming.parquet.write.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

import java.util.HashMap;
import java.util.Map;

public class JsonWriteSupport extends WriteSupport<JsonNode> {
    private Map<String, String> extraMetaData;
    private ObjectNodeWriter groupWriter;
    protected MessageType schema = null;

    public JsonWriteSupport() {
        extraMetaData = new HashMap<>();
    }

    public JsonWriteSupport(MessageType schema) {
        this();
        this.schema = schema;
    }

    public JsonWriteSupport(MessageType schema, Map<String, String> extraMetaData) {
        this.extraMetaData = extraMetaData;
        this.schema = schema;
    }

    @Override
    public WriteContext init(Configuration configuration) {
        if(schema == null){
            // TODO
//            Class<?> schemaGroupClass = ConfigurationUtil.getClassFromConfig(configuration, GenericParquetGroup.CFX_PARQUET_OUTPUT_SCHEMA, GenericParquetGroup.class);
//            try{
//                schema = ((GenericParquetGroup)schemaGroupClass.newInstance()).getSchema();
//            }catch (Exception e){
//                throw new RuntimeException("Unknown schema for parquet Output.", e);
//            }
        }
        return new WriteContext(schema, this.extraMetaData);
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
