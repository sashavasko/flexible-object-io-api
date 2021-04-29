package org.sv.flexobject.adapter;

import org.sv.flexobject.OutAdapter;
import org.sv.flexobject.stream.Sink;
import org.sv.flexobject.stream.Source;
import org.sv.flexobject.translate.Translator;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.function.Supplier;

public abstract class GenericOutAdapter<T> implements OutAdapter {

    public enum PARAMS {
        sink,
        recordFactory,
        recordClass,
        fieldNameTranslator
    }

    protected Supplier<T> recordFactory = null;
    protected Class<? extends T> recordClass = null;
    protected T currentRecord = null;
    protected Sink sink = null;
    protected Translator fieldNameTranslator = null;

    public GenericOutAdapter() {
    }

    public GenericOutAdapter(Sink sink) {
        this.sink = sink;
    }

    public GenericOutAdapter(Supplier<T> recordFactory) {
        this(null, recordFactory);
    }

    public GenericOutAdapter(Sink sink, Supplier<T> recordFactory) {
        this.sink = sink;
        this.recordFactory = recordFactory;
    }

    public GenericOutAdapter(Class<? extends T> recordClass) {
        this.recordClass = recordClass;
    }

    public GenericOutAdapter(Sink sink, Class<? extends T> recordClass) {
        this.recordClass = recordClass;
        this.sink = sink;
    }

    public void setParam(String key, Object value){
        setParam(PARAMS.valueOf(key), value);
    }

    public GenericOutAdapter<T> setParam(PARAMS key, Object value){
        if (PARAMS.sink == key && value != null && value instanceof Sink)
            sink = (Sink) value;
        else if (PARAMS.recordFactory == key && value != null && value instanceof Supplier)
            recordFactory = (Supplier<T>) value;
        else if (PARAMS.recordClass == key && value != null && value instanceof Class)
            recordClass = (Class<? extends T>) value;
        else if (PARAMS.fieldNameTranslator == key && value != null && value instanceof Translator)
            fieldNameTranslator = (Translator) value;
        return this;
    }

    public T createRecord() {
        try {
            return recordFactory != null ? recordFactory.get() : recordClass.newInstance();
        } catch (Exception e){
            throw new RuntimeException("Failed to create output record", e);
        }
    }

    protected T getCurrent(){
        if (currentRecord == null)
            currentRecord = createRecord();
        return currentRecord;
    }

    protected Object convertRecordForSink(T record){
        return record;
    }

    @Override
    public String translateOutputFieldName(String fieldName) {
        return fieldNameTranslator != null ? fieldNameTranslator.apply(fieldName) : fieldName;
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
