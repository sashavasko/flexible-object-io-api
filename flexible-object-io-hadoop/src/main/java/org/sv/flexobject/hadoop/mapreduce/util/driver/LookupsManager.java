package org.sv.flexobject.hadoop.mapreduce.util.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

public abstract class LookupsManager {

    public static final String CFX_LOOKUPS_MANAGER = "mapreduce.lookups.manager";

    public static boolean isConfigured(Configuration config) {
        return config.get(CFX_LOOKUPS_MANAGER) != null;
    }

    public static boolean isConfigured(JobContext context) {
        return isConfigured(context.getConfiguration());
    }

    public static boolean isConfigured(OutputProducer driver) {
        return isConfigured(driver.getConfiguration());
    }

    public abstract void cacheLookupsImp(OutputProducer driver);
    public void loadLookupsImp(TaskInputOutputContext context){};

    public static LookupsManager getInstance(Configuration config) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        String className = config.get(CFX_LOOKUPS_MANAGER);
        if (className != null)
            return (LookupsManager) Class.forName(className, true, Thread.currentThread().getContextClassLoader()).newInstance();
        return null;
    }

    public static void cacheLookups(OutputProducer driver) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        getInstance(driver.getConfiguration()).cacheLookupsImp(driver);
    }

    public static void loadLookups(TaskInputOutputContext context) throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        getInstance(context.getConfiguration()).loadLookupsImp(context);
    }
}
