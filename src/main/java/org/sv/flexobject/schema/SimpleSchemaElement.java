package org.sv.flexobject.schema;

public class SimpleSchemaElement implements SchemaElement {
    FieldDescriptor fieldDescriptor;

    public SimpleSchemaElement(Class<?> dataClass, Enum<?> e) throws NoSuchFieldException {
        fieldDescriptor = FieldDescriptor.fromEnum(dataClass, e);
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
