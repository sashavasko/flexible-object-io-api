package org.sv.flexobject.stream;


import org.sv.flexobject.Savable;
import org.sv.flexobject.adapter.GenericOutAdapter;
import org.sv.flexobject.io.Writer;
import org.sv.flexobject.stream.sinks.SingleValueSink;

import java.lang.reflect.InvocationTargetException;

public class Exporter<T> {

    private SingleValueSink<T> buffer = new SingleValueSink<>();
    private GenericOutAdapter<T> adapter;
    private Writer writer;
    private Sink<T> sink;

    public Exporter(Class adapterClass, Writer writerInstance, Sink<T> sink) throws InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        this.adapter = GenericOutAdapter.build(adapterClass, buffer);
        this.writer = writerInstance;
        this.sink = sink;
    }

    public boolean consume(Savable dbObject) throws Exception {
        if (dbObject == null)
            return false;

        if (writer == null)
            dbObject.save(adapter);
        else
            writer.convert(dbObject, adapter);
        T data = buffer.get();
        sink.put(data);
        return true;
    }

    public void setAdapterParam(String key, Object value) {
        adapter.setParam(key, value);
    }

    protected Exporter(GenericOutAdapter<T> adapter, Writer writer, Sink<T> sink) {
        this.adapter = adapter;
        this.writer = writer;
        this.sink = sink;
    }

    public Sink<T> getSink(){
        return sink;
    }

}
