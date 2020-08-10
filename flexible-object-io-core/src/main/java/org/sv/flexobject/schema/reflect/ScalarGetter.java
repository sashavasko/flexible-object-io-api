package org.sv.flexobject.schema.reflect;

import org.sv.flexobject.util.FunctionWithException;

public class ScalarGetter extends FieldWrapper implements FunctionWithException {

    public ScalarGetter(Class<?> clazz, String fieldName) {
        super(clazz, fieldName);
    }

    @Override
    public Object apply(Object input) throws Exception {
        return getValue(input);
    }
}
