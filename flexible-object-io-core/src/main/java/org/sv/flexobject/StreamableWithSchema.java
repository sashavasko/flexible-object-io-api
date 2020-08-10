package org.sv.flexobject;

import org.sv.flexobject.schema.Schema;
import org.sv.flexobject.schema.SchemaElement;
import org.sv.flexobject.schema.SchemaException;

public class StreamableWithSchema<T extends SchemaElement> implements Streamable {

    Schema schema;

    public StreamableWithSchema() {
        schema = Schema.getRegisteredSchema(getClass());
    }

    public StreamableWithSchema(T[] fields) throws NoSuchFieldException, SchemaException {
        if (!Schema.isRegisteredSchema(getClass())){
            schema = new Schema(getClass(), fields);
        }
    }

    public StreamableWithSchema(Enum<?>[] fields) throws NoSuchFieldException, SchemaException {
        if (!Schema.isRegisteredSchema(getClass())){
            schema = new Schema(getClass(), fields);
        }
    }


    public Schema getSchema(){
        if (schema == null)
            schema = Schema.getRegisteredSchema(getClass());
        return schema;
    }

    @Override
    public boolean load(InAdapter input) throws Exception {
        getSchema().clear(this);
        return getSchema().load(this, input) != null;
    }

    @Override
    public boolean save(OutAdapter output) throws Exception {
        return getSchema().save(this, output);
    }

    public Object get(T field) throws Exception {
        if (getSchema().isInferred())
            throw new SchemaException("Generic Getters and Setters only available in Streamable classes with explicit or annotated schema. Class " + schema.getName());
        return field.getDescriptor().get(this);
    }

    public Object get(Enum field) throws Exception {
        if (getSchema().isInferred())
            throw new SchemaException("Generic Getters and Setters only available in Streamable classes with explicit or annotated schema. Class " + schema.getName());
        return getSchema().getFieldDescriptor(field).get(this);
    }

    public void set(T field, Object value) throws Exception {
        if (getSchema().isInferred())
            throw new SchemaException("Generic Getters and Setters only available in Streamable classes with explicit or annotated schema. Class " + schema.getName());
        field.getDescriptor().set(this, value);
    }

    public void set(Enum field, Object value) throws Exception {
        if (getSchema().isInferred())
            throw new SchemaException("Generic Getters and Setters only available in Streamable classes with explicit or annotated schema. Class " + schema.getName());
        getSchema().getFieldDescriptor(field).set(this, value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StreamableWithSchema other = (StreamableWithSchema) o;
        if (!getSchema().equals(other.getSchema()))
            return false;

        return getSchema().compareFields(this, other);
    }
}
