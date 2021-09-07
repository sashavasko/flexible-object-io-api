package org.sv.flexobject.hadoop.adapter;

import org.apache.hadoop.conf.Configuration;
import org.sv.flexobject.adapter.DynamicInAdapter;
import org.sv.flexobject.adapter.GenericInAdapter;
import org.sv.flexobject.stream.Source;
import org.sv.flexobject.stream.sources.SingleValueSource;
import org.sv.flexobject.translate.NamespaceTranslator;
import org.sv.flexobject.translate.SeparatorTranslator;
import org.sv.flexobject.translate.Translator;
import org.sv.flexobject.util.InstanceFactory;

public class ConfigurationInAdapter extends GenericInAdapter<Configuration> implements DynamicInAdapter {

    public ConfigurationInAdapter() {
    }

    public ConfigurationInAdapter(SingleValueSource<Configuration> source, String namespace) {
        super(source);
        setParam(PARAMS.fieldNameTranslator, getTranslator(namespace));
    }

    public static Translator getTranslator(String namespace) {
        return new SeparatorTranslator(".").andThen(new NamespaceTranslator(namespace));
    }

    @Override
    public Object get(Object translatedFieldName) {
        return getCurrent().get((String) translatedFieldName);
    }

    public static class Builder {
        Configuration configuration;
        Source<Configuration> source;
        Translator fieldNameTranslator;

        public Builder from(Configuration configuration){
            this.configuration = configuration;
            return this;
        }

        public Builder fromSource(Source<Configuration> source){
            this.source = source;
            return this;
        }

        public Builder translator(Translator fieldNameTranslator){
            this.fieldNameTranslator = fieldNameTranslator;
            return this;
        }

        public ConfigurationInAdapter build(){
            ConfigurationInAdapter adapter = InstanceFactory.get(ConfigurationInAdapter.class);
            if (fieldNameTranslator != null)
                adapter.setParam(GenericInAdapter.PARAMS.fieldNameTranslator, fieldNameTranslator);
            if (configuration != null)
                adapter.setParam(PARAMS.source, new SingleValueSource<>(configuration));
            else if (source != null)
                adapter.setParam(PARAMS.source, source);
            return adapter;
        }
    }

    public static Builder builder(){
        return InstanceFactory.get(Builder.class);
    }
}
