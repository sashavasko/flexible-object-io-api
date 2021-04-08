package org.sv.flexobject.schema;

import org.sv.flexobject.InAdapter;
import org.sv.flexobject.OutAdapter;
import org.sv.flexobject.StreamableWithSchema;

public class AbstractFieldDescriptor {
    protected String name;
    protected int order;

    public AbstractFieldDescriptor(String name, int order) {
        this.name = name;
        this.order = order;
    }

    public int getOrder() {
        return order;
    }

    public String getName() {
        return name;
    }

    public String getQualifiedName(Object o){
        return o.getClass().getName() + "." + name;
    }

    public void load(Object o, InAdapter adapter) throws SchemaException{
        throw new SchemaException("Class " + getClass().getName() + " does not implements load()");
    }

    public void save(Object o, OutAdapter adapter) throws SchemaException{
        throw new SchemaException("Class " + getClass().getName() + " does not implements save()");
    }

    public Object get(Object o) throws SchemaException{
        throw new SchemaException("Class " + getClass().getName() + " does not implements get()");
    }

    // Setter always merges data
    public void set(Object o, Object value) throws SchemaException{
        throw new SchemaException("Class " + getClass().getName() + " does not implements set()");
    }

    public void clear(Object o) throws SchemaException{
        throw new SchemaException("Class " + getClass().getName() + " does not implements clear()");
    }

    public boolean isEmpty(StreamableWithSchema o){
        return false;
    }
}
