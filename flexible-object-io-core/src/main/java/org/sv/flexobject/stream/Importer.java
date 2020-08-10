package org.sv.flexobject.stream;

import org.sv.flexobject.Loadable;
import org.sv.flexobject.adapter.GenericInAdapter;
import org.sv.flexobject.io.Reader;
import org.sv.flexobject.stream.sources.SingleValueSource;

import java.lang.reflect.InvocationTargetException;

public class Importer<T> {

    private SingleValueSource<T> source = new SingleValueSource<>();
    private GenericInAdapter<T> adapter;
    private Reader reader;

    public Importer(Class adapterClass, Reader readerInstance) throws InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        this.adapter = GenericInAdapter.build(adapterClass, source);
        this.reader = readerInstance;
    }

    public void setAdapterParam(String key, Object value) {
        adapter.setParam(key, value);
    }

    public Loadable produce(T data) throws Exception {
        if (data == null)
            return null;

        source.setValue(data);
        adapter.next();
        Loadable datum = reader.create();
        reader.convert(adapter, datum);
        return datum;
    }

    public Source<T> getSource(){
        return source;
    }

    protected Importer(SingleValueSource<T> source, GenericInAdapter<T> adapter, Reader reader) {
        this.source = source;
        this.adapter = adapter;
        this.reader = reader;
    }
}
