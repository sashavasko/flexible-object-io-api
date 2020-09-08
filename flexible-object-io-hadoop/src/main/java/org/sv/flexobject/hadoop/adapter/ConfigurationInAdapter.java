package org.sv.flexobject.hadoop.adapter;

import org.apache.hadoop.conf.Configuration;
import org.sv.flexobject.adapter.DynamicInAdapter;
import org.sv.flexobject.adapter.GenericInAdapter;
import org.sv.flexobject.stream.sources.SingleValueSource;
import org.sv.flexobject.translate.NamespaceTranslator;
import org.sv.flexobject.translate.SeparatorTranslator;
import org.sv.flexobject.translate.Translator;

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
        return getCurrent().get(translateInputFieldName((String) translatedFieldName));
    }

    public static ConfigurationInAdapter forValue(Configuration configuration, String namespace) throws Exception {
        if (configuration != null) {
            SingleValueSource<Configuration> source = new SingleValueSource<>(configuration);
            return new ConfigurationInAdapter(source, namespace);
        }
        return null;
    }


}
