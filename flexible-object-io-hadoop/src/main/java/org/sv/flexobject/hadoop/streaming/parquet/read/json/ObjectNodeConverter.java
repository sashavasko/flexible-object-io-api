package org.sv.flexobject.hadoop.streaming.parquet.read.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;
import org.sv.flexobject.hadoop.streaming.parquet.read.SchemedGroupConverter;
import org.sv.flexobject.hadoop.streaming.parquet.read.SchemedPrimitiveConverter;

public class ObjectNodeConverter extends SchemedGroupConverter<ObjectNode> {

    public ObjectNodeConverter(GroupType schema) {
        super(schema);
    }

    public ObjectNodeConverter(ObjectNodeConverter parent, String parentName, GroupType type) {
        super(parent, parentName, type);
    }

    @Override
    protected SchemedPrimitiveConverter<ObjectNode> newPrimitiveConverter(PrimitiveType type) {
        return new ValueNodeConverter(type);
    }

    @Override
    protected SchemedGroupConverter<ObjectNode> newGroupConverter(String parentName, GroupType type) {
        return new ObjectNodeConverter(this, parentName, type);
    }

    protected ObjectNode newGroupInstance(){
        return JsonNodeFactory.instance.objectNode();
    }

    @Override
    protected void addChildGroup(String name, ObjectNode child) {
        ObjectNode parent = getCurrentRecord();
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
