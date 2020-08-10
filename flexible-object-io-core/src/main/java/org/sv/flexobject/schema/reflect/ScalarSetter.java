package org.sv.flexobject.schema.reflect;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.sv.flexobject.schema.SchemaException;
import org.sv.flexobject.util.BiConsumerWithException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ScalarSetter extends FieldWrapper implements BiConsumerWithException {

    public ScalarSetter(Class<?> clazz, String fieldName) {
        super(clazz, fieldName);
    }

    public List jsonArrayToTypedList(ArrayNode arrayNode) throws Exception {
        List list = new ArrayList();
        for (JsonNode elemNode : arrayNode){
            list.add(getType().convert(elemNode));
        }
        return list;
    }

    @Override
    public void accept(Object dataObject, Object value) throws Exception {
        if (value instanceof ArrayNode && !JsonNode.class.isAssignableFrom(getFieldClass())){
            ArrayNode arrayNode = (ArrayNode) value;
            if (getStructure() == STRUCT.array){
                int idx = 0;
                Object[] array = (Object[]) getValue(dataObject);
                if (array == null)
                    throw new SchemaException("Arrays must be initialized in data objects with Schema. Field " + fieldName + " in class " + clazz.getName());

                for (JsonNode elemNode : arrayNode){
                    array[idx++] = getType().convert(elemNode);
                    if (idx >= array.length)
                        return;
                }
            }else if (getStructure() == STRUCT.list){
                int idx = 0;
                List list = (List) getValue(dataObject);
                for (JsonNode elemNode : arrayNode){
                    Object convertedValue = getType().convert(elemNode);
                    if (list.size() <= idx)
                        list.add(convertedValue);
                    else
                        list.set(idx, convertedValue);
                    idx++;
                }
            } else
                throw new SchemaException("Cannot set scalar field " + fieldName + " from Json Array");
        } else if (value instanceof ObjectNode && !JsonNode.class.isAssignableFrom(getFieldClass())){
            if (getStructure() != STRUCT.map)
                throw new SchemaException("Json ObjectNode can only be converted to a Map. Field " + fieldName + " in class " + clazz.getName());
            Map map = (Map) getValue(dataObject);
            ObjectNode objectNode = (ObjectNode) value;
            Iterator<Map.Entry<String, JsonNode>> fields = objectNode.fields();
            while (fields.hasNext()){
                Map.Entry<String,JsonNode> entry = fields.next();
                map.put(entry.getKey(), getType().convert(entry.getValue()));
            }
        } else
            setValue(dataObject, getType().convert(value));
    }
}
