package org.sv.flexobject.util;

import java.util.HashMap;
import java.util.Map;

public class InstanceFactory {
    private static Map<Class, Object> map = new HashMap();

    public static void set(Class<?> clazz, Object objectToStore) {
        map.put(clazz, objectToStore);
    }

    public static <T> T get(Class<T> clazz) {
        T instance = (T) map.get(clazz);
        if (instance == null) {
            try {
                instance = clazz.newInstance();
            } catch (Exception e) {
                throw new RuntimeException("Failed to instantiate object of " + clazz.getName(), e);
            }
        }

        return instance;
    }

    public static void reset(){
        map.clear();
    }
}
