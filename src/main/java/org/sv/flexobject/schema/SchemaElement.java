package org.sv.flexobject.schema;

public interface SchemaElement<T extends Enum> {
    FieldDescriptor getDescriptor();
}
