package org.sv.flexobject.schema.reflect;

import org.sv.flexobject.json.JsonOutputAdapter;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.util.BiConsumerWithException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ValueNode;

import java.util.function.Function;

public class JsonSetter extends JsonFieldWrapper implements BiConsumerWithException {

    protected Function<Object, JsonNode> jsonNodeMaker = JsonSetter::textSetter;

    public JsonSetter(Class<?> clazz, String fieldName, String path, Function<Object, JsonNode> jsonNodeMaker) {
        super(clazz, fieldName, path);
        this.jsonNodeMaker = jsonNodeMaker;
    }

    public JsonSetter(Class<?> clazz, String fieldName, String path, DataTypes type) {
        super(clazz, fieldName, path);
        switch (type) {
            case string : jsonNodeMaker = JsonSetter::textSetter; break;
            case jsonNode : jsonNodeMaker = n->(JsonNode)n; break;
            case int32 : jsonNodeMaker = JsonSetter::intSetter; break;
            case bool : jsonNodeMaker = JsonSetter::booleanSetter; break;
            case int64 : jsonNodeMaker = JsonSetter::longSetter; break;
            case float64 : jsonNodeMaker = JsonSetter::doubleSetter; break;
            case date : jsonNodeMaker = JsonOutputAdapter::dateToJsonNode; break;
            case timestamp : jsonNodeMaker = JsonOutputAdapter::timestampToJsonNode; break;
            case localDate : jsonNodeMaker = JsonOutputAdapter::localDateToJsonNode; break;
        }
    }

    public static ValueNode textSetter(Object value){
        return JsonNodeFactory.instance.textNode((String) value);
    }

    public static ValueNode intSetter(Object value){
        return JsonNodeFactory.instance.numberNode(((Number)value).intValue());
    }

    public static ValueNode longSetter(Object value){
        return JsonNodeFactory.instance.numberNode(((Number)value).longValue());
    }

    public static ValueNode doubleSetter(Object value){
        return JsonNodeFactory.instance.numberNode(((Number)value).doubleValue());
    }

    public static ValueNode booleanSetter(Object value){
        return JsonNodeFactory.instance.booleanNode((boolean)value);
    }

    @Override
    public void accept(Object input, Object value) throws Exception {
        Object o = getValue(input);
        ObjectNode owner = (ObjectNode) findNode((ObjectNode) o, 0, true);
        owner.set(jsonFieldName, jsonNodeMaker.apply(value));
    }
}
