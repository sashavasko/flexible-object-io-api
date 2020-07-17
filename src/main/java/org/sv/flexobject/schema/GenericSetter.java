package org.sv.flexobject.schema;

import org.sv.flexobject.util.BiConsumerWithException;

import java.lang.reflect.Field;

public class GenericSetter implements BiConsumerWithException {

    String fieldName;
    Field field;
    Class<?> clazz;

    public GenericSetter(Class<?> clazz, String fieldName) {
        this.fieldName = fieldName;
        this.clazz = clazz;
    }

    @Override
    public void accept(Object object, Object value) throws Exception {
        if (field == null) {
            field = clazz.getDeclaredField(fieldName);
            field.setAccessible(true);
        }
        field.set(object, value);
    }
}
