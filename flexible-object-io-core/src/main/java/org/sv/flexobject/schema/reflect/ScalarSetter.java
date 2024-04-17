package org.sv.flexobject.schema.reflect;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.schema.SchemaException;
import org.sv.flexobject.util.BiConsumerWithException;
import org.sv.flexobject.util.InstanceFactory;

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

    protected boolean isNull(Object value){
        if (value == null)
            return true;
        if (value instanceof JsonNode
                && ((JsonNode)value).isNull())
            return true;
        if (value instanceof ObjectNode
                && ((ObjectNode)value).isEmpty())
            return true;
        return false;
    }
    @Override
    public void accept(Object dataObject, Object value) throws Exception {
        Class<? extends Streamable> valueClass = getValueClass();
        if (isNull(value)) {
            setValue(dataObject, null);
            return;
        }

        if (value instanceof Collection) {
            Collection collection = (Collection) value;
            if (getStructure() == STRUCT.array) {
                Object[] array = (Object[]) getValue(dataObject);
                if (array == null)
                    throw new SchemaException(getQualifiedName() + ": Arrays must be initialized in data objects with Schema. Field " + fieldName + " in class " + clazz.getName());
                int idx = 0;
                for (Object elem : collection) {
                    if (elem == null || array[idx] instanceof Streamable) {
                        array[idx] = elem;
                    } else {
                        array[idx] = getType().convert(elem);
                    }
                    idx++;
                    if (idx >= array.length)
                        return;
                }
            } else {
                if (getValue(dataObject) instanceof Set){
                    Set set = (Set) getValue(dataObject);
                    set.clear();
                    set.addAll(collection);
                }else
                    setValue(dataObject, value);
            }
        } else if (value instanceof Map) {
            if (getStructure() != STRUCT.map) {
                throw new SchemaException(getQualifiedName() + ": Map Objects can only be converted to a Map or Json");
            }

            Map<Object,Object> valueMap = (Map) value;
            Map map = (Map) getField().get(dataObject);
            if (map != null){
                map.clear();
                for (Map.Entry<Object,Object> entry : valueMap.entrySet()){
                    map.put(keyType.convert(entry.getKey()), entry.getValue());
                }
            }else
                setValue(dataObject, valueMap);
        } else if (value.getClass().isArray()) {
            if (getType() == DataTypes.binary){
                byte[] bytes = DataTypes.binaryConverter(value);
                setValue(dataObject, bytes);
            } else {
                Object[] valueArray = (Object[]) value;
                if (getStructure() == STRUCT.array) {
                    Object[] objectArray = (Object[]) getValue(dataObject);
                    int i = 0;
                    for (; i < objectArray.length && i < valueArray.length; ++i) {
                        Object item = valueArray[i];
                        if (item == null)
                            objectArray[i] = null;
                        else if (getValueClass() != null && getValueClass().isAssignableFrom(item.getClass())) {
                            objectArray[i] = item;
                        } else {
                            objectArray[i] = getType().convert(item);
                        }
                    }
                    for (; i < objectArray.length; ++i) {
                        objectArray[i] = null;
                    }
                } else if (getStructure() == STRUCT.list) {
                    setValue(dataObject, Arrays.asList(valueArray));
                }
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
                    } else if (array[idx] instanceof Streamable){
                        ((Streamable)array[idx]).fromJson(elemNode);
                    } else if (array[idx] == null && elemNode.isContainerNode()){
                        if (valueClass == null)
                            throw new SchemaException(getQualifiedName() + ": Arrays of substructures must be initialized with instances, or ValueType annotation must be used.");
                        array[idx] = InstanceFactory.get(valueClass);
                        ((Streamable)array[idx]).fromJson(elemNode);
                    }else {
                        array[idx] = getType().convert(elemNode);
                    }
                    idx++;
                    if (idx >= array.length)
                        return;
                }
            }else if (getStructure() == STRUCT.list){
                int idx = 0;
                Collection collection = (Collection) getValue(dataObject);
                for (JsonNode elemNode : arrayNode){
                    Object convertedValue = null;
                    if (valueClass != null) {
                        if (collection instanceof List) {
                            List list = (List) collection;
                            if (list.size() > idx)
                                convertedValue = list.get(idx);
                        }
                        if (convertedValue == null)
                            convertedValue = InstanceFactory.get(valueClass);
                        ((Streamable)convertedValue).fromJson(elemNode);
                    }else {
                        convertedValue = getType().convert(elemNode);
                    }


                    if (collection.size() <= idx || !(collection instanceof List))
                        collection.add(convertedValue);
                    else {
                        ((List)collection).set(idx, convertedValue);
                    }
                    idx++;
                }
            } else
                throw new SchemaException(getQualifiedName() + ": Cannot set scalar field from Json Array. Perhaps you need @ValueType annotation?");
        } else if (value instanceof ObjectNode && !JsonNode.class.isAssignableFrom(getFieldClass())){
            if (Streamable.class.isAssignableFrom(getFieldClass())){
                Streamable subStruct = (Streamable) getValue(dataObject);
                if (subStruct == null) {
                    subStruct = (Streamable) InstanceFactory.get(getFieldClass());
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
                            convertedValue = InstanceFactory.get(valueClass);
                        ((Streamable)convertedValue).fromJson(node);
                    }else {
                        convertedValue = getType().convert(node);
                    }

                    if (keyType != null && keyType != DataTypes.string){
                        Object convertedKey = keyType.convert(key);
                        map.put(convertedKey, convertedValue);
                    } else
                        map.put(key, convertedValue);
                }
            }
        } else if (getValueClass() != null && getValueClass().isAssignableFrom(value.getClass())){
            setValue(dataObject, value);
        } else {
            if (getStructure() == STRUCT.list) {
                Object field = getField().get(dataObject);
                if (field == null)
                    throw new SchemaException("List fields must be pre-initialized");
                ((Collection)getField().get(dataObject)).add(getType().convert(value));
            }else
                setValue(dataObject, getType().convert(value));
        }
    }
}
