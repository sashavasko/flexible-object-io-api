package org.sv.flexobject.hadoop.streaming.parquet.read.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.sv.flexobject.hadoop.streaming.parquet.ParquetSchema;
import org.sv.flexobject.hadoop.streaming.parquet.ParquetSourceConf;

import java.util.Map;

public class JsonReadSupport extends ReadSupport<JsonNode> {

    public JsonReadSupport() {
    }

    @Override
    public ReadContext init(InitContext context) {
        ParquetSourceConf conf = new ParquetSourceConf().from(context.getConfiguration());
        if (conf.getDataClass() != null) {
            return new ReadContext(ParquetSchema.forClass(conf.getDataClass()));
        }else
            return new ReadContext(context.getFileSchema());
    }

    @Override
    public RecordMaterializer<JsonNode> prepareForRead(Configuration configuration, Map<String, String> map, MessageType messageType, ReadContext readContext) {
        MessageType schema = readContext.getRequestedSchema();
        return new JsonRecordConverter(schema);
    }

    public static void setJsonNodeWithRepetition(ObjectNode parent, String name, JsonNode child){
        if (parent.has(name)){
            JsonNode current = parent.get(name);
            ArrayNode array;
            if (current.isArray()) {
                array = (ArrayNode) current;
            } else {
                array = JsonNodeFactory.instance.arrayNode();
                array.add(current);
                parent.set(name, array);
            }
            array.add(child);
        } else
            parent.set(name, child);
    }

}
