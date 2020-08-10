package org.sv.flexobject.schema.reflect;

import org.sv.flexobject.util.BiConsumerWithException;

import java.util.HashMap;
import java.util.Map;

public class MapSetter extends FieldWrapper implements BiConsumerWithException {

    Object keyInMap;

    public MapSetter(Class<?> clazz, String fieldName, Object keyInMap) {
        super(clazz, fieldName);
        this.keyInMap = keyInMap;
    }

    @Override
    public void accept(Object input, Object value) throws Exception {
        Object fieldValue = getValue(input);

        if (fieldValue == null){
            fieldValue = new HashMap<>();
            setValue(input, fieldValue);
        }

        ((Map) fieldValue).put(keyInMap, getType().convert(value));
    }
}
