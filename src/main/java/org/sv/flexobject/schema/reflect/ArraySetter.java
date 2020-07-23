package org.sv.flexobject.schema.reflect;

import org.sv.flexobject.util.BiConsumerWithException;

import java.util.List;

public class ArraySetter extends FieldWrapper implements BiConsumerWithException {

    int indexInArray;

    public ArraySetter(Class<?> clazz, String fieldName, int indexInArray) {
        super(clazz, fieldName);
        this.indexInArray = indexInArray;
    }

    @Override
    public void accept(Object input, Object value) throws Exception {
        Object fieldValue = getValue(input);

        Object convertedValue = getType().convert(value);
        if (fieldValue instanceof List){
            List list = ((List)fieldValue);
            while (list.size() <= indexInArray)
                list.add(null);

            list.set(indexInArray, convertedValue);
        } else
            ((Object[]) fieldValue)[indexInArray] = convertedValue;
    }
}
