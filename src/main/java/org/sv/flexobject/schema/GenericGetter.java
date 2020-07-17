package org.sv.flexobject.schema;

import org.sv.flexobject.util.FunctionWithException;

import java.lang.reflect.Field;

public class GenericGetter implements FunctionWithException {

    String fieldName;
    Field field;
    Class<?> clazz;

    public GenericGetter(Class<?> clazz, String fieldName) {
        this.fieldName = fieldName;
        this.clazz = clazz;
    }

    @Override
    public Object apply(Object input) throws Exception {
        if (field == null) {
            field = clazz.getDeclaredField(fieldName);
            field.setAccessible(true);
        }
        return field.get(input);
    }
}
