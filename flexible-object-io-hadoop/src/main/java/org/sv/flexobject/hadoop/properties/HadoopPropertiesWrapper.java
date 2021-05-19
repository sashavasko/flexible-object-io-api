package org.sv.flexobject.hadoop.properties;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.sv.flexobject.hadoop.HadoopTask;
import org.sv.flexobject.hadoop.adapter.ConfigurationInAdapter;
import org.sv.flexobject.hadoop.adapter.ConfigurationOutAdapter;
import org.sv.flexobject.hadoop.adapter.SparkConfInAdapter;
import org.sv.flexobject.hadoop.adapter.SparkConfOutAdapter;
import org.sv.flexobject.properties.PropertiesWrapper;
import org.sv.flexobject.schema.annotations.NonStreamableField;

public abstract class HadoopPropertiesWrapper<T extends HadoopPropertiesWrapper> extends PropertiesWrapper<T> implements Configurable {

    @NonStreamableField
    private String namespace;

    @NonStreamableField
    private Configuration configuration;

    public HadoopPropertiesWrapper() {
        this(getDefaultNamespace());
    }

    public HadoopPropertiesWrapper(String namespace) {
        this.namespace = namespace;
    }

    public String getSubNamespace(){
        return null;
    }

    public String getNamespace() {
        return getSubNamespace() == null ? namespace : namespace + "." + getSubNamespace();
    }

    public T from (Configuration configuration) {
        if (configuration != null) {
            this.configuration = configuration;
            try {
                return from(ConfigurationInAdapter.forValue(configuration, getNamespace()));
            } catch (Exception e) {
                if (e instanceof RuntimeException)
                    throw (RuntimeException)e;
                throw new RuntimeException("Configuration failed with exception", e);
            }
        }
        return (T) this;
    }

    public T from (SparkConf configuration) {
        if (configuration != null) {
            try {
                return from(SparkConfInAdapter.forValue(configuration, getNamespace()));
            } catch (Exception e) {
                if (e instanceof RuntimeException)
                    throw (RuntimeException)e;
                throw new RuntimeException("Configuration failed with exception", e);
            }
        }
        return (T) this;
    }

    public boolean update(Configuration configuration) throws Exception {
        ConfigurationOutAdapter.update(configuration, getNamespace(), this::save);
        return true;
    }

    public boolean update(SparkConf configuration) throws Exception {
        SparkConfOutAdapter.update(configuration, getNamespace(), this::save);
        return true;
    }

    public String getSettingName(String fieldName){
        return ConfigurationInAdapter.getTranslator(getNamespace()).apply(fieldName);
    }

    public static String getDefaultNamespace(){
        return HadoopTask.DEFAULT_NAMESPACE;
    }

    @Override
    public void setConf(Configuration configuration) {
        from(configuration);
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }
}
