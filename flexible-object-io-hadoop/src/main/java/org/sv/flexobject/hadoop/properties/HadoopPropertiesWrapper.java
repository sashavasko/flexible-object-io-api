package org.sv.flexobject.hadoop.properties;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.sv.flexobject.hadoop.HadoopTask;
import org.sv.flexobject.hadoop.adapter.ConfigurationInAdapter;
import org.sv.flexobject.hadoop.adapter.ConfigurationOutAdapter;
import org.sv.flexobject.hadoop.adapter.SparkConfInAdapter;
import org.sv.flexobject.hadoop.adapter.SparkConfOutAdapter;
import org.sv.flexobject.properties.Namespace;
import org.sv.flexobject.properties.NamespacePropertiesWrapper;
import org.sv.flexobject.properties.PropertiesWrapper;
import org.sv.flexobject.schema.annotations.NonStreamableField;

public class HadoopPropertiesWrapper<T extends HadoopPropertiesWrapper> extends NamespacePropertiesWrapper<T> implements Configurable {

    public static final Namespace SPARK_NAMESPACE = Namespace.forPath(".", "spark");

    @NonStreamableField
    private Configuration configuration;

    public HadoopPropertiesWrapper() {
        super();
    }

    @Override
    public T setDefaults() {
        return (T) this;
    }

    public HadoopPropertiesWrapper(String subNamespace) {
        super(subNamespace);
    }

    public HadoopPropertiesWrapper(Namespace parent, String subNamespace) {
        super(parent, subNamespace);
    }

    protected String diagnostics(){
        return "Configuration " + getClass().getName() + "{" + toString() + "}";
    }

    public String addDiagnostics(String message){
        return message + " in " + diagnostics();
    }

    public RuntimeException runtimeException(String message, Exception e){
        if (e == null)
            return new RuntimeException(addDiagnostics(message));
        if (e instanceof RuntimeException)
            return (RuntimeException)e;
        return new RuntimeException(addDiagnostics(message), e);
    }

    public RuntimeException runtimeException(Logger logger, String message, Exception e){
        if ( e == null)
            logger.error(addDiagnostics(message));
        else
            logger.error(addDiagnostics(message), e);
        return runtimeException(message, e);
    }

    public T from (Configuration configuration) {
        if (configuration != null) {
            this.configuration = configuration;
            try {
                ConfigurationInAdapter.builder().from(configuration).translator(getTranslator()).build().consume(this::load);
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
                SparkConfInAdapter.builder().from(configuration).translator(getTranslator()).build().consume(this::load);
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

    @Override
    public void setConf(Configuration configuration) {
        from(configuration);
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }
}
