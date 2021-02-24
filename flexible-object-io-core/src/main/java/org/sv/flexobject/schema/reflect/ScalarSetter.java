package org.sv.flexobject.schema.reflect;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.json.JsonInputAdapter;
import org.sv.flexobject.schema.SchemaException;
import org.sv.flexobject.util.BiConsumerWithException;

import java.util.*;

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
        Class<? extends StreamableWithSchema> valueClass = getValueClass();
        if (value instanceof Map) {
            if (getStructure() != STRUCT.map)
                throw new SchemaException(getQualifiedName() + ": Map Objects can only be converted to a Map");

            Map valueMap = (Map) value;
            setValue(dataObject, valueMap);
        } else if (value.getClass().isArray()) {
            Object[] valueArray = (Object[]) value;
            if (getStructure() == STRUCT.array) {
                Object[] objectArray = (Object[]) getValue(dataObject);
                int i = 0;
                for (; i < objectArray.length && i < valueArray.length; ++i){
                    Object item = valueArray[i];
                    if (item == null)
                        objectArray[i] = null;
                    else if (getValueClass() != null && getValueClass().isAssignableFrom(item.getClass())){
                        objectArray[i] = item;
                    } else {
                        objectArray[i] = getType().convert(item);
                    }
                }
                for (; i < objectArray.length; ++i){
                    objectArray[i] = null;
                }
            } else if (getStructure() == STRUCT.list){
                setValue(dataObject, Arrays.asList(valueArray));
            }
        }else if (value instanceof ArrayNode && !JsonNode.class.isAssignableFrom(getFieldClass())){
            ArrayNode arrayNode = (ArrayNode) value;
            if (getStructure() == STRUCT.array){
                int idx = 0;
                Object[] array = (Object[]) getValue(dataObject);
                if (array == null)
                    throw new SchemaException(getQualifiedName() + ": Arrays must be initialized in data objects with Schema. Field " + fieldName + " in class " + clazz.getName());

                for (JsonNode elemNode : arrayNode){
                    if (elemNode.isNull()) {
                        array[idx] = null;
                    } else if (array[idx] instanceof StreamableWithSchema){
                        ((StreamableWithSchema)array[idx]).fromJson(elemNode);
                    } else if (array[idx] == null && elemNode.isContainerNode()){
                        if (valueClass == null)
                            throw new SchemaException(getQualifiedName() + ": Arrays of substructures must be initialized with instances, or ValueType annotation must be used.");
                        array[idx] = valueClass.newInstance();
                        ((StreamableWithSchema)array[idx]).fromJson(elemNode);
                    }else {
                        array[idx] = getType().convert(elemNode);
                    }
                    idx++;
                    if (idx >= array.length)
                        return;
                }
            }else if (getStructure() == STRUCT.list){
                int idx = 0;
                List list = (List) getValue(dataObject);
                for (JsonNode elemNode : arrayNode){
                    Object convertedValue = null;
                    if (valueClass != null) {
                        if (list.size() > idx)
                            convertedValue = list.get(idx);
                        if (convertedValue == null)
                            convertedValue = valueClass.newInstance();
                        ((StreamableWithSchema)convertedValue).fromJson(elemNode);
                    }else {
                        convertedValue = getType().convert(elemNode);
                    }


                    if (list.size() <= idx)
                        list.add(convertedValue);
                    else
                        list.set(idx, convertedValue);
                    idx++;
                }
            } else
                throw new SchemaException(getQualifiedName() + ": Cannot set scalar field from Json Array");
        } else if (value instanceof ObjectNode && !JsonNode.class.isAssignableFrom(getFieldClass())){
            if (StreamableWithSchema.class.isAssignableFrom(getFieldClass())){
                StreamableWithSchema subStruct = (StreamableWithSchema) getValue(dataObject);
                if (subStruct == null) {
                    subStruct = (StreamableWithSchema) getFieldClass().newInstance();
                    setValue(dataObject, subStruct);
                }
                subStruct.fromJson((JsonNode) value);
            }else {
                if (getStructure() != STRUCT.map)
                    throw new SchemaException(getQualifiedName() + ": Json ObjectNode can only be converted to a Map");
                Map map = (Map) getValue(dataObject);
                ObjectNode objectNode = (ObjectNode) value;
                Iterator<Map.Entry<String, JsonNode>> fields = objectNode.fields();
                while (fields.hasNext()) {
                    Map.Entry<String, JsonNode> entry = fields.next();
                    Object convertedValue = null;
                    String key = entry.getKey();
                    JsonNode node = entry.getValue();
                    if (valueClass != null && node.isObject()){
                        convertedValue = map.get(key);
                        if (convertedValue == null)
                            convertedValue = valueClass.newInstance();
                        ((StreamableWithSchema)convertedValue).fromJson(node);
                    }else {
                        convertedValue = getType().convert(node);
                    }

                    map.put(key, convertedValue);
                }
            }
        } else if (getValueClass() != null && getValueClass().isAssignableFrom(value.getClass())){
            setValue(dataObject, value);
        } else {
            setValue(dataObject, getType().convert(value));
        }
    }
}
