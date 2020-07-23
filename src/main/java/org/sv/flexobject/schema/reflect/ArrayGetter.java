package org.sv.flexobject.schema.reflect;

import org.sv.flexobject.util.FunctionWithException;

import java.util.List;

public class ArrayGetter extends FieldWrapper  implements FunctionWithException {

    protected int indexInArray;

    public ArrayGetter(Class<?> clazz, String fieldName, int indexInArray) {
        super(clazz, fieldName);
        this.indexInArray = indexInArray;
    }

    @Override
    public Object apply(Object input) throws Exception {
        Object value = getValue(input);
        if (value instanceof List){
            return ((List)value).get(indexInArray);
        }
        return ((Object[]) value)[indexInArray];
    }
}
