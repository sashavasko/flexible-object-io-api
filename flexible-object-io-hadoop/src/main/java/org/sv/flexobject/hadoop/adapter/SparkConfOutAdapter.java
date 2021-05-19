package org.sv.flexobject.hadoop.adapter;

import org.apache.spark.SparkConf;
import org.sv.flexobject.adapter.DynamicOutAdapter;
import org.sv.flexobject.adapter.GenericOutAdapter;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.stream.sinks.SingleValueSink;
import org.sv.flexobject.util.ConsumerWithException;

public class SparkConfOutAdapter extends GenericOutAdapter<SparkConf> implements DynamicOutAdapter {
    public SparkConfOutAdapter() {
        super(new SingleValueSink());
    }

    public SparkConfOutAdapter(SparkConf configuration, String namespace){
        this();
        setParam(PARAMS.fieldNameTranslator, ConfigurationInAdapter.getTranslator(namespace));
        currentRecord = configuration;
    }

    @Override
    public Object put(String translatedFieldName, Object value) throws Exception {
        if (value == null)
            getCurrent().remove(translatedFieldName);
        else
            getCurrent().set(translatedFieldName, DataTypes.stringConverter(value));
        return value;
    }

    static public void update(SparkConf configuration, String namespace, ConsumerWithException<SparkConfOutAdapter, Exception> consumer) throws Exception {
        SparkConfOutAdapter adapter = new SparkConfOutAdapter(configuration, namespace);
        consumer.accept(adapter);
    }
}
