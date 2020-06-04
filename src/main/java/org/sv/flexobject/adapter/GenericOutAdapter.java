package org.sv.flexobject.adapter;

import org.sv.flexobject.OutAdapter;
import org.sv.flexobject.stream.Sink;
import org.sv.flexobject.stream.Source;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public abstract class GenericOutAdapter<T> implements OutAdapter {
    protected T currentRecord = null;
    protected Sink sink;

    private GenericOutAdapter() {
        sink = null;
    }

    public GenericOutAdapter(Sink sink) {
        this.sink = sink;
    }

    public void setParam(String key, Object value){
        if ("sink".equals(key) && value != null && value instanceof Sink)
            sink = (Sink) value;
    }

    public abstract T createRecord();

    protected T getCurrent(){
        if (currentRecord == null)
            currentRecord = createRecord();
        return currentRecord;
    }

    protected Object convertRecordForSink(T record){
        return record;
    }

    @Override
    public GenericOutAdapter<T> save() throws Exception {
        if (currentRecord == null)
            throw new IllegalArgumentException("Cannot save empty record");
        sink.put(convertRecordForSink(currentRecord));
        currentRecord = null;
        return this;
    }

    @Override
    public boolean shouldSave() {
        return currentRecord != null;
    }

    public boolean hasOutput(){
        return sink.hasOutput();
    }

    public Sink getSink() {
        return sink;
    }

    public static GenericOutAdapter build(Class clazz, Sink sink) throws IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        Constructor constructor = clazz.getConstructor(new Class[]{Sink.class});
        GenericOutAdapter adapter = (GenericOutAdapter) constructor.newInstance(sink);
        adapter.sink = sink;
        return adapter;
    }

}
