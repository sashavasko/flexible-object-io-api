package org.sv.flexobject.schema;

import java.lang.reflect.Field;

public class SimpleSchemaElement implements SchemaElement {
    FieldDescriptor fieldDescriptor;

    public SimpleSchemaElement(Class<?> dataClass, Enum<?> e) throws NoSuchFieldException {
        fieldDescriptor = FieldDescriptor.fromEnum(dataClass, e);
    }

    public SimpleSchemaElement(Class<?> dataClass, Field field, int order) {
        fieldDescriptor = FieldDescriptor.fromField(dataClass, field, order);
    }

    @Override
    public FieldDescriptor getDescriptor() {
        return fieldDescriptor;
    }

    @Override
    public void setDescriptor(FieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }
}
