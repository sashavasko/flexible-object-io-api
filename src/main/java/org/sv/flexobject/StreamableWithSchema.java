package org.sv.flexobject;

import org.sv.flexobject.schema.Schema;
import org.sv.flexobject.schema.SchemaElement;
import org.sv.flexobject.schema.SchemaRegistry;

public class StreamableWithSchema<T extends SchemaElement> implements Streamable {

    Schema schema;

    public StreamableWithSchema(T[] fields) {
        String schemaName = getClass().getName();
        if (!SchemaRegistry.getInstance().hasSchema(schemaName)){
            schema = new Schema(schemaName, fields);
            SchemaRegistry.getInstance().registerSchema(schema);
        } else
            schema = SchemaRegistry.getInstance().getSchema(schemaName);
    }

    @Override
    public boolean load(InAdapter input) throws Exception {
        return schema.load(this, input);
    }

    @Override
    public boolean save(OutAdapter output) throws Exception {
        return schema.save(this, output);
    }

    public Object get(T field) throws Exception {
        return field.getDescriptor().get(this);
    }

    public void set(T field, Object value) throws Exception {
        field.getDescriptor().set(this, value);
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
