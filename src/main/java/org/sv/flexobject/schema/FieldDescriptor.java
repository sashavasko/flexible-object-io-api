package org.sv.flexobject.schema;

import org.sv.flexobject.*;
import org.sv.flexobject.util.BiConsumerWithException;
import org.sv.flexobject.util.FunctionWithException;

public class FieldDescriptor {

    protected String name;
    protected DataTypes type;
    protected FunctionWithException getter;
    protected BiConsumerWithException setter;
    protected int order;

    public FieldDescriptor(String name, DataTypes type, FunctionWithException getter, BiConsumerWithException setter, int order) {
        this.name = name;
        this.type = type;
        this.getter = getter;
        this.setter = setter;
        this.order = order;
    }

    public FieldDescriptor(Class<?> clazz, String name, DataTypes type, int order) {
        this.name = name;
        this.type = type;
        this.order = order;
        this.setter = new GenericSetter(clazz, name);
        this.getter = new GenericGetter(clazz, name);
    }

    public FieldDescriptor(Class<?> clazz, String name, DataTypes type, FunctionWithException getter, int order) {
        this.name = name;
        this.type = type;
        this.order = order;
        this.setter = new GenericSetter(clazz, name);
        this.getter = getter;
    }

    public void load(Loadable o, InAdapter adapter) throws Exception {
        setter.accept(o, type.get(adapter, name));
    }

    public void save(Savable o, OutAdapter adapter) throws Exception {
        type.set(adapter, name, getter.apply(o));
    }

    public Object get(Object o) throws Exception {
        return getter.apply(o);
    }

    public void set(Object o, Object value) throws Exception {
        setter.accept(o, value);
    }

    public int getOrder() {
        return order;
    }

    public String getName() {
        return name;
    }

    public DataTypes getType() {
        return type;
    }
}
