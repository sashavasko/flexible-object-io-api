package org.sv.flexobject.schema;

import java.lang.reflect.Field;

public class SimpleSchemaElement implements SchemaElement {
    AbstractFieldDescriptor fieldDescriptor;

    public SimpleSchemaElement(Class<?> dataClass, Enum<?> e) throws NoSuchFieldException, SchemaException {
        fieldDescriptor = FieldDescriptor.fromEnum(dataClass, e);
    }

    public SimpleSchemaElement(Class<?> dataClass, Field field, int order) {
        fieldDescriptor = FieldDescriptor.fromField(dataClass, field, order);
    }

    public SimpleSchemaElement(AbstractFieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }

    @Override
    public AbstractFieldDescriptor getDescriptor() {
        return fieldDescriptor;
    }

    @Override
    public void setDescriptor(AbstractFieldDescriptor fieldDescriptor) {
        this.fieldDescriptor = fieldDescriptor;
    }
}