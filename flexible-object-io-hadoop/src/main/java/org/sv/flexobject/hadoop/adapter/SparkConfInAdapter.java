package org.sv.flexobject.hadoop.adapter;

import org.apache.spark.SparkConf;
import org.sv.flexobject.adapter.DynamicInAdapter;
import org.sv.flexobject.adapter.GenericInAdapter;
import org.sv.flexobject.stream.sources.SingleValueSource;

import java.util.NoSuchElementException;

public class SparkConfInAdapter extends GenericInAdapter<SparkConf> implements DynamicInAdapter {

    public SparkConfInAdapter() {
    }

    public SparkConfInAdapter(SingleValueSource<SparkConf> source, String namespace) {
        super(source);
        setParam(PARAMS.fieldNameTranslator, ConfigurationInAdapter.getTranslator(namespace));
    }

    @Override
    public Object get(Object translatedFieldName) {
        try {
            return getCurrent().get((String) translatedFieldName);
        }catch (NoSuchElementException e){
            return null;
        }
    }
    public static SparkConfInAdapter forValue(SparkConf configuration, String namespace) throws Exception {
        if (configuration != null) {
            SingleValueSource<SparkConf> source = new SingleValueSource<>(configuration);
            return new SparkConfInAdapter(source, namespace);
        }
        return null;
    }
}
