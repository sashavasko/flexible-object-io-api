package org.sv.flexobject.adapter;


import org.sv.flexobject.InAdapter;
import org.sv.flexobject.stream.Source;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public abstract class GenericInAdapter<T> implements InAdapter {

    T currentRecord = null;
    Source<T> source;

    private GenericInAdapter() {
        source = null;
    }

    public GenericInAdapter(Source<T> source) {
        this.source = source;
    }

    public static GenericInAdapter build(Class clazz, Source source) throws IllegalAccessException, InstantiationException, InvocationTargetException, NoSuchMethodException {
        Constructor constructor = clazz.getConstructor(new Class[]{Source.class});
        GenericInAdapter adapter = (GenericInAdapter) constructor.newInstance(source);
        adapter.source = source;
        return adapter;
    }

    public void setParam(String key, Object value){
        if ("source".equals(key) && value != null && value instanceof Source)
            source = (Source<T>) value;
    }

    public T getCurrent(){
        return currentRecord;
    }

    @Override
    public boolean next() throws Exception {
        if (source.isEOF())
            return false;
        currentRecord = source.get();
        return currentRecord != null;
    }

    public Source<T> getSource() {
        return source;
    }

    @Override
    public void ack() {
        source.ack();
    }

    @Override
    public void setEOF() {
        source.setEOF();
    }

    @Override
    public void close() throws Exception {
        source.close();
    }
}
