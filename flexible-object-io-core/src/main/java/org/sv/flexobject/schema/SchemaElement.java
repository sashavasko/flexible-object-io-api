package org.sv.flexobject.schema;

public interface SchemaElement<T extends Enum> {
    AbstractFieldDescriptor getDescriptor();

    void setDescriptor(AbstractFieldDescriptor fieldDescriptor);

    default int getOrder() {
        return getDescriptor().getOrder();
    }

    default String getName() {
        return getDescriptor().getName();
    }
}

