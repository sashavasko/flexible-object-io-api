package org.sv.flexobject.hadoop.streaming.parquet.read.json;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.parquet.schema.MessageType;
import org.sv.flexobject.hadoop.streaming.parquet.read.SchemedGroupConverter;
import org.sv.flexobject.hadoop.streaming.parquet.read.SchemedReadSupport;

public class JsonReadSupport extends SchemedReadSupport<ObjectNode> {

    public JsonReadSupport() {
    }

    public JsonReadSupport(MessageType schema) {
        super(schema);
    }

    @Override
    public SchemedGroupConverter<ObjectNode> newGroupConverter(MessageType schema) {
        return new ObjectNodeConverter(schema);
    }
}
