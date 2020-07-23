package org.sv.flexobject.schema.reflect;

import org.sv.flexobject.util.FunctionWithException;

import java.util.Map;

public class MapGetter extends FieldWrapper  implements FunctionWithException {

    protected Object keyInMap;

    public MapGetter(Class<?> clazz, String fieldName, Object keyInMap) {
        super(clazz, fieldName);
        this.keyInMap = keyInMap;
    }

    @Override
    public Object apply(Object input) throws Exception {
        return ((Map)getValue(input)).get(keyInMap);
    }
}
