package org.sv.flexobject.io;

import org.sv.flexobject.Loadable;

import java.util.HashMap;
import java.util.Map;

public class GenericReader implements Reader {

    protected static Map<String, GenericReader> instances = new HashMap<>();

    Class<? extends Loadable> datumClass = null;

    public GenericReader(Class<? extends Loadable> datumClass) {
        this.datumClass = datumClass;
    }

    @Override
    public Loadable create() throws  Exception{
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

}
