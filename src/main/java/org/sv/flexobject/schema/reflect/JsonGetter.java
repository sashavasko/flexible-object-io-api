package org.sv.flexobject.schema.reflect;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ValueNode;
import org.sv.flexobject.json.JsonInputAdapter;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.util.FunctionWithException;

import java.util.function.Function;

public class JsonGetter extends JsonFieldWrapper  implements FunctionWithException {

    protected Function<ValueNode, Object> jsonGetter;

    public JsonGetter(Class<?> clazz, String fieldName, String path, Function<ValueNode, Object> jsonGetter) {
        super(clazz, fieldName, path);
        this.jsonGetter = jsonGetter;
    }

    public JsonGetter(Class<?> clazz, String fieldName, String path, DataTypes type) {
        super(clazz, fieldName, path);
        switch (type) {
            case string: jsonGetter = JsonNode::asText; break;
            case jsonNode: jsonGetter = n->n; break;
            case int32: jsonGetter = JsonNode::asInt; break;
            case int64: jsonGetter = JsonNode::asLong; break;
            case float64: jsonGetter = JsonNode::asDouble; break;
            case bool: jsonGetter = JsonNode::asBoolean; break;
            case date: jsonGetter = JsonInputAdapter::jsonNodeToDate; break;
            case timestamp: jsonGetter = JsonInputAdapter::jsonNodeToTimestamp; break;
            case localDate: jsonGetter = JsonInputAdapter::jsonNodeToLocalDate; break;
        }
    }

    @Override
    public Object apply(Object input) throws Exception {
        ValueNode node = findValueInPath((ObjectNode) getValue(input), 0);
        return node == null ? null : jsonGetter.apply(node);
    }
}
