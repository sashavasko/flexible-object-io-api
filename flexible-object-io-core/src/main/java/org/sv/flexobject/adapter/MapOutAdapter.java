package org.sv.flexobject.adapter;

import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.stream.Sink;
import org.sv.flexobject.stream.sinks.SingleValueSink;
import org.sv.flexobject.util.ConsumerWithException;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class MapOutAdapter extends GenericOutAdapter<Map> implements DynamicOutAdapter {

    public MapOutAdapter() {
        super(()->new HashMap<String, Object>());
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

    public static Map produce(MapOutAdapter adapter, StreamableWithSchema data) throws Exception {
        return produce(adapter, data::save);
    }

    public static Map produce(MapOutAdapter adapter, ConsumerWithException<MapOutAdapter, Exception> consumer) throws Exception {
        SingleValueSink<Map> sink = new SingleValueSink<>();
        adapter.setParam(PARAMS.sink, sink);

        consumer.accept(adapter);

        return sink.get();
    }

    public static Map produce(Class<? extends Map> outputClass, ConsumerWithException<MapOutAdapter, Exception> consumer) throws Exception {
        return produce(new MapOutAdapter(outputClass), consumer);
    }

    public static Map produce(Class<? extends Map> outputClass, StreamableWithSchema data) throws Exception {
        return produce(new MapOutAdapter(outputClass), data::save);
    }

    public static Map produce(Supplier<Map> mapFactory, ConsumerWithException<MapOutAdapter, Exception> consumer) throws Exception {
        return produce(new MapOutAdapter(mapFactory), consumer);
    }

    public static Map produce(Supplier<Map> mapFactory, StreamableWithSchema data) throws Exception {
        return produce(new MapOutAdapter(mapFactory), data::save);
    }

    public static Map produceHashMap(ConsumerWithException<MapOutAdapter, Exception> consumer) throws Exception {
        return produce(new MapOutAdapter(), consumer);
    }

    public static Map produceHashMap(StreamableWithSchema data) throws Exception {
        return produce(new MapOutAdapter(), data::save);
    }
}
