package org.sv.flexobject.hadoop.properties;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.sv.flexobject.OutAdapter;
import org.sv.flexobject.hadoop.HadoopBatchEnvironment;
import org.sv.flexobject.hadoop.adapter.ConfigurationInAdapter;
import org.sv.flexobject.hadoop.adapter.ConfigurationOutAdapter;
import org.sv.flexobject.properties.PropertiesWrapper;
import org.sv.flexobject.schema.annotations.NonStreamableField;

public class HadoopPropertiesWrapper<T extends HadoopPropertiesWrapper> extends PropertiesWrapper<T> implements Configurable {

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
                e.printStackTrace();
                System.exit(-1);
            }
        }
        return (T) this;
    }

    public boolean update(Configuration configuration) throws Exception {
        ConfigurationOutAdapter.update(configuration, getNamespace(), this::save);
        return true;
    }

    public String getSettingName(String fieldName){
        return ConfigurationInAdapter.getTranslator(getNamespace()).apply(fieldName);
    }

    public static String getDefaultNamespace(){
        return HadoopBatchEnvironment.DEFAULT_NAMESPACE;
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
