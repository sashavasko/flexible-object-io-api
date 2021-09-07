package org.sv.flexobject.hadoop.adapter;

import org.apache.spark.SparkConf;
import org.sv.flexobject.adapter.DynamicInAdapter;
import org.sv.flexobject.adapter.GenericInAdapter;
import org.sv.flexobject.stream.Source;
import org.sv.flexobject.stream.sources.SingleValueSource;
import org.sv.flexobject.translate.Translator;
import org.sv.flexobject.util.InstanceFactory;

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

    public static class Builder {
        SparkConf configuration;
        Source<SparkConf> source;
        Translator fieldNameTranslator;

        public Builder from(SparkConf configuration){
            this.configuration = configuration;
            return this;
        }

        public Builder fromSource(Source<SparkConf> source){
            this.source = source;
            return this;
        }

        public Builder translator(Translator fieldNameTranslator){
            this.fieldNameTranslator = fieldNameTranslator;
            return this;
        }

        public SparkConfInAdapter build(){
            SparkConfInAdapter adapter = InstanceFactory.get(SparkConfInAdapter.class);
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
