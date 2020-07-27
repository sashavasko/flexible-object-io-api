package org.sv.flexobject.io;

import org.sv.flexobject.InAdapter;
import org.sv.flexobject.Loadable;
import org.sv.flexobject.schema.Schema;

import java.util.HashMap;
import java.util.Map;

public class GenericReader implements Reader {

    protected static Map<String, GenericReader> instances = new HashMap<>();

    Class<? extends Loadable> datumClass = null;
    Schema schema;

    public GenericReader(Class<? extends Loadable> datumClass) {
        this.datumClass = datumClass;
    }

    public GenericReader(Schema schema) {
        this.schema = schema;
    }

    @Override
    public Loadable create() throws  Exception{
        if (datumClass == null)
            datumClass = (Class<? extends Loadable>) this.getClass().getClassLoader().loadClass(schema.getName());

        return datumClass.newInstance();
    }

    public static GenericReader getInstance(Class<? extends Loadable> datumClass) {
        GenericReader instance = instances.get(datumClass.getName());
        if (instance == null) {
            instance = new GenericReader(datumClass);
            instances.put(datumClass.getName(), instance);
        }
        return instance;
    }

    @Override
    public Loadable convert(InAdapter input, Loadable datum) throws Exception {
        if (schema != null)
            return schema.loadFields(datum, input) ? datum : null;
        else
            return datum.load(input) ? datum : null;
    }
}
