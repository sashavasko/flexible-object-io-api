package org.sv.flexobject.adapter;

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
    public Object put(String fieldName, Object value) throws Exception{
        return getCurrent().put(fieldName, value);
    }

    public static Map produce(Class<? extends Map> outputClass, ConsumerWithException<MapOutAdapter, Exception> consumer) throws Exception {
        SingleValueSink<Map> sink = new SingleValueSink<>();
        MapOutAdapter adapter = new MapOutAdapter(sink, outputClass);

        consumer.accept(adapter);

        return sink.get();
    }
}
