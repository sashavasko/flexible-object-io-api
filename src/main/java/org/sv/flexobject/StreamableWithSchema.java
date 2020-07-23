package org.sv.flexobject;

import org.sv.flexobject.schema.Schema;
import org.sv.flexobject.schema.SchemaElement;
import org.sv.flexobject.schema.SchemaRegistry;

public class StreamableWithSchema<T extends SchemaElement> implements Streamable {

    Schema schema;

    public StreamableWithSchema() throws NoSuchFieldException {
        schema = Schema.getRegisteredSchema(getClass());
    }

    public StreamableWithSchema(T[] fields) throws NoSuchFieldException {
        this();
        if (schema == null){
            schema = new Schema(getClass(), fields);
            SchemaRegistry.getInstance().registerSchema(schema);
        }
    }

    public StreamableWithSchema(Enum<?>[] fields) throws NoSuchFieldException {
        this();
        if (schema == null){
            schema = new Schema(getClass(), fields);
            SchemaRegistry.getInstance().registerSchema(schema);
        }
    }

    @Override
    public boolean load(InAdapter input) throws Exception {
        schema.clear(this);
        return schema.load(this, input);
    }

    @Override
    public boolean save(OutAdapter output) throws Exception {
        return schema.save(this, output);
    }

    public Object get(T field) throws Exception {
        return field.getDescriptor().get(this);
    }

    public Object get(Enum field) throws Exception {
        return schema.getFieldDescriptor(field).get(this);
    }

    public void set(T field, Object value) throws Exception {
        field.getDescriptor().set(this, value);
    }

    public void set(Enum field, Object value) throws Exception {
        schema.getFieldDescriptor(field).set(this, value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StreamableWithSchema other = (StreamableWithSchema) o;
        if (!schema.equals(other.schema))
            return false;

        return schema.compareFields(this, other);
    }


}
