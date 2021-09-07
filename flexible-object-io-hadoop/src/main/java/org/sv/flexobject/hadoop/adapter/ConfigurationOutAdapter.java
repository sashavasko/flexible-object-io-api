package org.sv.flexobject.hadoop.adapter;

import org.apache.hadoop.conf.Configuration;
import org.sv.flexobject.adapter.DynamicOutAdapter;
import org.sv.flexobject.adapter.GenericInAdapter;
import org.sv.flexobject.adapter.GenericOutAdapter;
import org.sv.flexobject.json.JsonOutputAdapter;
import org.sv.flexobject.properties.Namespace;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.stream.Sink;
import org.sv.flexobject.stream.sinks.SingleValueSink;
import org.sv.flexobject.translate.Translator;
import org.sv.flexobject.util.ConsumerWithException;
import org.sv.flexobject.util.InstanceFactory;

public class ConfigurationOutAdapter extends GenericOutAdapter<Configuration> implements DynamicOutAdapter {

    public ConfigurationOutAdapter() {
        super(new SingleValueSink());
    }

    @Override
    public Object put(String translatedFieldName, Object value) throws Exception {
        if (value == null)
            getCurrent().unset(translatedFieldName);
        else
            getCurrent().set(translatedFieldName, DataTypes.stringConverter(value));
        return value;
    }

    public static class Builder {
        Configuration configuration;
        Sink<Configuration> sink;
        Translator fieldNameTranslator;

        public Builder to(Configuration configuration){
            this.configuration = configuration;
            return this;
        }

        public Builder toSink(Sink<Configuration> sink){
            this.sink = sink;
            return this;
        }

        public Builder translator(Translator fieldNameTranslator){
            this.fieldNameTranslator = fieldNameTranslator;
            return this;
        }

        public ConfigurationOutAdapter build(){
            ConfigurationOutAdapter adapter = InstanceFactory.get(ConfigurationOutAdapter.class);
            if (fieldNameTranslator != null)
                adapter.setParam(GenericOutAdapter.PARAMS.fieldNameTranslator, fieldNameTranslator);
            if (configuration != null) {
                adapter.currentRecord = configuration;
            }else if (sink != null)
                adapter.setParam(PARAMS.sink, sink);
            return adapter;
        }
    }

    public static Builder builder(){
        return InstanceFactory.get(Builder.class);
    }

    static public void update(Configuration configuration, Namespace namespace, ConsumerWithException<ConfigurationOutAdapter, Exception> consumer) throws Exception {
        ConfigurationOutAdapter adapter = builder().to(configuration).translator(namespace.getTranslator()).build();
        consumer.accept(adapter);
    }
}
