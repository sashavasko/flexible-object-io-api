package org.sv.flexobject.hadoop;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;

import java.util.HashMap;
import java.util.Map;

public class HadoopInstanceFactory {

    private static Configuration conf = new Configuration(true);
    private static Map<String, Object> singletons = new HashMap<>();

    public static Configuration getConf() {
        return conf;
    }

    public static void setConf(Configuration conf) {
        HadoopInstanceFactory.conf = conf;
    }

    public static Object get(final String propertyName) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        return get(propertyName, conf);
    }

    public static Object get(final String propertyName, Configuration conf) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        String className = conf.get(propertyName);
        if (StringUtils.isBlank(className))
            return null;

        Class clazz = conf.getClassByName(className);
        if (clazz == null)
            return null;

        Object instance = clazz.newInstance();

        if (instance != null && instance instanceof Configurable)
            ((Configurable) instance).setConf(conf);

        return instance;
    }

    public static Object getSingleton(final String propertyName) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        return getSingleton(propertyName, conf);
    }

    public static Object getSingleton(final String propertyName, Configuration conf) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        String className = conf.get(propertyName);
        if (StringUtils.isBlank(className))
            return null;

        if (singletons.containsKey(className))
            return singletons.get(className);

        Object instance = get(propertyName, conf);
        if (instance != null)
            singletons.put(className, instance);

        return instance;
    }
}