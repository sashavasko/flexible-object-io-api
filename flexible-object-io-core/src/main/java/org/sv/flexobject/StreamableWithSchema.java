package org.sv.flexobject;

import org.sv.flexobject.schema.Schema;
import org.sv.flexobject.schema.SchemaElement;
import org.sv.flexobject.schema.SchemaException;

/**
 * More sophisticated implementation of Streamable,
 * caching schema so it does not have to be looked up for every call
 *
 * Also adds more flexible ways to generate Schema using an enum T extends SchemaElement
 * which allows to define custom getters and setters for things like array element implemented as a field
 * @param <T> extends SchemaElement
 */
public class StreamableWithSchema<T extends SchemaElement> extends StreamableImpl {

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

    @Override
    public Schema getSchema(){
        if (schema == null)
            schema = Schema.getRegisteredSchema(getClass());
        return schema;
    }

    public Object get(T field) throws Exception {
//        if (getSchema().isInferred())
//            throw new SchemaException("Generic Getters and Setters only available in Streamable classes with explicit or annotated schema. Class " + schema.getName());
        return field.getDescriptor().get(this);
    }

    public void set(T field, Object value) throws Exception {
//        if (getSchema().isInferred())
//            throw new SchemaException("Generic Getters and Setters only available in Streamable classes with explicit or annotated schema. Class " + schema.getName());
        field.getDescriptor().set(this, value);
    }

}
