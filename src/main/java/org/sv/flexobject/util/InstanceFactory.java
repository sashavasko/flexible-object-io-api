package org.sv.flexobject.util;

import org.sv.flexobject.OutAdapter;
import org.sv.flexobject.sql.SqlOutAdapter;

public class InstanceFactory {


    public static Object get(Class clazz) {
        try {
            return clazz.newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }

    //TODO
    public static Object get(Class clazz, Object[] parameters) {
        return null;
    }
}
