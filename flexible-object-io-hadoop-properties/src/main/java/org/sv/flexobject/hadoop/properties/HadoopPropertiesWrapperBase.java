package org.sv.flexobject.hadoop.properties;

import org.sv.flexobject.hadoop.adapter.ConfigurationInAdapter;
import org.sv.flexobject.hadoop.adapter.ConfigurationOutAdapter;
import org.sv.flexobject.properties.Namespace;
import org.sv.flexobject.properties.NamespacePropertiesWrapper;
import org.sv.flexobject.schema.annotations.NonStreamableField;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

public class HadoopPropertiesWrapperBase<T extends HadoopPropertiesWrapperBase> extends NamespacePropertiesWrapper<T> implements Configurable {
    @NonStreamableField
    private Configuration configuration;

    public HadoopPropertiesWrapperBase() {
        super();
    }

    @Override
    public T setDefaults() {
        @SuppressWarnings("unchecked") T casted = (T) this;
        return (T) casted;
    }

    public HadoopPropertiesWrapperBase(String subNamespace) {
        super(subNamespace);
    }

    public HadoopPropertiesWrapperBase(Namespace parent, String subNamespace) {
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
                throw runtimeException("Configuration failed with exception", e);
            }
        }
        @SuppressWarnings("unchecked") T casted = (T) this;
        return casted;
    }

    public boolean update(Configuration configuration) throws Exception {
        ConfigurationOutAdapter.update(configuration, getNamespace(), this::save);
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
