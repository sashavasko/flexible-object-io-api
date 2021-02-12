package org.sv.flexobject.hadoop.adapter;

import org.apache.hadoop.conf.Configuration;
import org.sv.flexobject.adapter.DynamicOutAdapter;
import org.sv.flexobject.adapter.GenericInAdapter;
import org.sv.flexobject.adapter.GenericOutAdapter;
import org.sv.flexobject.json.JsonOutputAdapter;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.stream.sinks.SingleValueSink;
import org.sv.flexobject.util.ConsumerWithException;

public class ConfigurationOutAdapter extends GenericOutAdapter<Configuration> implements DynamicOutAdapter {

    public ConfigurationOutAdapter() {
        super(new SingleValueSink());
    }

    public ConfigurationOutAdapter(Configuration configuration, String namespace){
        this();
        setParam(GenericOutAdapter.PARAMS.fieldNameTranslator, ConfigurationInAdapter.getTranslator(namespace));
        currentRecord = configuration;
    }

    @Override
    public Object put(String translatedFieldName, Object value) throws Exception {
        if (value == null)
            getCurrent().unset(translatedFieldName);
        else
            getCurrent().set(translatedFieldName, DataTypes.stringConverter(value));
        return value;
    }

    static public void update(Configuration configuration, String namespace, ConsumerWithException<ConfigurationOutAdapter, Exception> consumer) throws Exception {
        ConfigurationOutAdapter adapter = new ConfigurationOutAdapter(configuration, namespace);
        consumer.accept(adapter);
    }

}
