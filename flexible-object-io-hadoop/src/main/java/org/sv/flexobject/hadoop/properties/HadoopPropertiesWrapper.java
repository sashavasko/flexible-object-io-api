package org.sv.flexobject.hadoop.properties;

import org.apache.hadoop.conf.Configuration;
import org.sv.flexobject.hadoop.HadoopBatchEnvironment;
import org.sv.flexobject.hadoop.adapter.ConfigurationInAdapter;
import org.sv.flexobject.properties.PropertiesWrapper;

public class HadoopPropertiesWrapper<T extends HadoopPropertiesWrapper> extends PropertiesWrapper<T> {

    private String namespace;

    public HadoopPropertiesWrapper() {
        namespace = getDefaultNamespace();
    }

    public HadoopPropertiesWrapper(String namespace) {
        this.namespace = namespace;
    }

    public static String getDefaultNamespace(){
        return HadoopBatchEnvironment.DEFAULT_NAMESPACE;
    }

    public String getSubNamespace(){
        return null;
    }

    public String getNamespace() {
        return getSubNamespace() == null ? namespace : namespace + "." + getSubNamespace();
    }

    public T from (Configuration configuration) {
        if (configuration != null) {
            try {
                return from(ConfigurationInAdapter.forValue(configuration, getNamespace()));
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }
        return (T) this;
    }

    public String getSettingName(String fieldName){
        return ConfigurationInAdapter.getTranslator(getNamespace()).apply(fieldName);
    }

}
