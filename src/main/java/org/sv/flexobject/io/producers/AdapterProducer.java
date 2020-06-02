package org.sv.flexobject.io.producers;

import org.sv.flexobject.InAdapter;
import org.sv.flexobject.Loadable;
import org.sv.flexobject.adapter.GenericInAdapter;
import org.sv.flexobject.io.CloseableProducer;
import org.sv.flexobject.io.Reader;
import org.sv.flexobject.stream.Source;

import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class AdapterProducer extends CloseableProducer {
    InAdapter adapter;
    Reader reader;

    public AdapterProducer(Source source, Class<? extends GenericInAdapter> clazz, Reader reader) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        adapter = GenericInAdapter.build(clazz, source);
        this.reader = reader;
    }

    public AdapterProducer(InAdapter adapter, Reader reader) {
        this.adapter = adapter;
        this.reader = reader;
    }

    @Override
    public void unsafeClose() throws Exception {
        adapter.close();
    }

    public Loadable produce(){
        Loadable datum = null;

        try {
            if (adapter.next()) {
                datum = reader.create();
                reader.convert(adapter, datum);
            }
        } catch (Exception e) {
            setException(e);
        }
        return datum;
    }

    @Override
    public void ack() {
        adapter.ack();
    }

    @Override
    public void setEOF() {
        adapter.setEOF();
    }

    public class AdapterIterator implements Iterator<Loadable>{

        Loadable datum = null;

        @Override
        public boolean hasNext() {
            datum = produce();
            return datum != null;
        }

        @Override
        public Loadable next() {
            if (datum == null)
                throw new NoSuchElementException();
            return datum;
        }
    }

    @Override
    public Iterator<Loadable> iterator() {
        return new AdapterIterator();
    }

}
