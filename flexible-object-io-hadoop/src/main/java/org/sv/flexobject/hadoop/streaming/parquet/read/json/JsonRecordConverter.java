package org.sv.flexobject.hadoop.streaming.parquet.read.json;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

public class JsonRecordConverter extends RecordMaterializer<JsonNode> {

    MessageType schema;
    ObjectNodeConverter root;

    public JsonRecordConverter(MessageType schema) {
        this.schema = schema;
        root = new ObjectNodeConverter(schema);
    }

    @Override
    public JsonNode getCurrentRecord() {
        return root.getCurrentRecord();
    }

    @Override
    public GroupConverter getRootConverter() {
        return root;
    }
}
