package org.sv.flexobject.adapter;

import org.sv.flexobject.Streamable;
import org.sv.flexobject.stream.Sink;
import org.sv.flexobject.stream.sinks.SingleValueSink;
import org.sv.flexobject.translate.Translator;
import org.sv.flexobject.util.ConsumerWithException;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class MapOutAdapter extends GenericOutAdapter<Map> implements DynamicOutAdapter {

    public MapOutAdapter() {
//        super(()->new HashMap<String, Object>());
    }

    public MapOutAdapter(Sink sink) {
        super(sink, ()->new HashMap<String, Object>());
    }

    public MapOutAdapter(Supplier<Map> recordFactory) {
        super(recordFactory);
    }

    public MapOutAdapter(Sink sink, Supplier<Map> recordFactory) {
        super(sink, recordFactory);
    }

    public MapOutAdapter(Class<? extends Map> recordClass) {
        super(recordClass);
    }

    public MapOutAdapter(Sink sink, Class<? extends Map> recordClass) {
        super(sink, recordClass);
    }

    @Override
    public Object put(String translatedFieldName, Object value) throws Exception{
        if (value == null) {
            getCurrent().remove(translatedFieldName);
            return null;
        } else
            return getCurrent().put(translatedFieldName, value);
    }

    public static Map produce(MapOutAdapter adapter, Streamable data) throws Exception {
        return produce(adapter, data::save);
    }

    public static Map produce(MapOutAdapter adapter, ConsumerWithException<MapOutAdapter, Exception> consumer) throws Exception {
        SingleValueSink<Map> sink = new SingleValueSink<>();
        adapter.setParam(PARAMS.sink, sink);
        return adapter.produce(consumer);
    }

    public Map produce(ConsumerWithException<MapOutAdapter, Exception> consumer) throws Exception {
        consumer.accept(this);
        return (Map) getSink().get();
    }

    public static Map produce(Class<? extends Map> outputClass, ConsumerWithException<MapOutAdapter, Exception> consumer) throws Exception {
        return produce(new MapOutAdapter(outputClass), consumer);
    }

    public static Map produce(Class<? extends Map> outputClass, Streamable data) throws Exception {
        return produce(new MapOutAdapter(outputClass), data::save);
    }

    public static Map produce(Supplier<Map> mapFactory, ConsumerWithException<MapOutAdapter, Exception> consumer) throws Exception {
        return produce(new MapOutAdapter(mapFactory), consumer);
    }

    public static Map produce(Supplier<Map> mapFactory, Streamable data) throws Exception {
        return produce(new MapOutAdapter(mapFactory), data::save);
    }

    public static Map produceHashMap(ConsumerWithException<MapOutAdapter, Exception> consumer) throws Exception {
        return produce(new MapOutAdapter(()->new HashMap<String, Object>()), consumer);
    }

    public static Map produceHashMap(Streamable data) throws Exception {
        return produce(new MapOutAdapter(()->new HashMap<String, Object>()), data::save);
    }

    public static class Builder {
        Class<? extends Map> outputClass;
        Supplier<Map> mapFactory;
        Sink<Map> sink;
        Translator fieldNameTranslator;

        public Builder forClass(Class<? extends Map> outputClass){
            this.outputClass = outputClass;
            return this;
        }

        public Builder withFactory(Supplier<Map> mapFactory){
            this.mapFactory = mapFactory;
            return this;
        }

        public Builder toSink(Sink<Map> sink){
            this.sink = sink;
            return this;
        }

        public Builder translator(Translator fieldNameTranslator){
            this.fieldNameTranslator = fieldNameTranslator;
            return this;
        }

        public MapOutAdapter build(){
            MapOutAdapter adapter = new MapOutAdapter();
            if (outputClass != null)
                adapter.setParam(PARAMS.recordClass, outputClass);
            if (mapFactory != null)
                adapter.setParam(PARAMS.recordFactory, mapFactory);
            if (fieldNameTranslator != null)
                adapter.setParam(PARAMS.fieldNameTranslator, fieldNameTranslator);
            if (sink != null)
                adapter.setParam(PARAMS.sink, sink);
            else
                adapter.setParam(PARAMS.sink, new SingleValueSink<>());
            return adapter;
        }
    }

    public static Builder builder(){
        return new Builder();
    }
}
