package org.sv.flexobject.io.producers;

import org.sv.flexobject.InAdapter;
import org.sv.flexobject.Loadable;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.adapter.GenericInAdapter;
import org.sv.flexobject.io.CloseableProducer;
import org.sv.flexobject.io.Reader;
import org.sv.flexobject.schema.Schema;
import org.sv.flexobject.stream.Source;
import org.sv.flexobject.util.InstanceFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class AdapterProducer<T extends Loadable> extends CloseableProducer<T> {
    InAdapter adapter;
    Reader<T> reader;

    public static class Builder {
        Source source;
        Class<? extends GenericInAdapter> adapterClass;
        Reader reader;
        InAdapter adapter;
        Class<? extends AdapterProducer> producerClass = AdapterProducer.class;

        private Builder() {
        }

        public Builder from(Source source){
            this.source = source;
            return this;
        }

        public Builder using(Class<? extends GenericInAdapter> adapterClass){
            this.adapterClass = adapterClass;
            return this;
        }

        public Builder using(InAdapter adapter){
            this.adapter = adapter;
            return this;
        }

        public Builder with(Reader reader){
            this.reader = reader;
            return this;
        }

        public Builder instanceOf(Class<? extends AdapterProducer> producerClass){
            this.producerClass = producerClass;
            return this;
        }

        public AdapterProducer build() {
            AdapterProducer producer = InstanceFactory.get(producerClass);
            producer.setReader(reader);
            if (adapter == null) {
                adapter = InstanceFactory.get(adapterClass);
                adapter.setParam(GenericInAdapter.PARAMS.source.name(), source);
                producer.setAdapter(adapter);
            }else {
                producer.setAdapter(adapter);
            }
            return producer;
        }
    }

    public static Builder builder(){
        return new Builder();
    }

    @Deprecated  // Use builder() instead
    public AdapterProducer(Source source, Class<? extends GenericInAdapter> adapterClass, Reader reader) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        adapter = GenericInAdapter.build(adapterClass, source);
        this.reader = reader;
    }

    @Deprecated  // Use builder() instead
    public AdapterProducer(InAdapter adapter, Reader reader) {
        this.adapter = adapter;
        this.reader = reader;
    }

    @Deprecated  // Use builder() instead
    public AdapterProducer(Source source, Class<? extends GenericInAdapter> adapterClass, Class<? extends StreamableWithSchema> dataClass) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        this(source, adapterClass, Schema.getRegisteredSchema(dataClass).getReader());
    }

    @Deprecated  // Use builder() instead
    public AdapterProducer(InAdapter adapter, Class dataClass) {
        this(adapter, Schema.getRegisteredSchema(dataClass).getReader());
    }

    public void setAdapter(InAdapter adapter) {
        this.adapter = adapter;
    }

    public void setReader(Reader<T> reader) {
        this.reader = reader;
    }

    @Override
    public void unsafeClose() throws Exception {
        adapter.close();
    }

    public T produce(){
        T datum = null;

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

    public class AdapterIterator implements Iterator<T>{

        T datum = null;

        @Override
        public boolean hasNext() {
            datum = produce();
            return datum != null;
        }

        @Override
        public T next() {
            if (datum == null)
                throw new NoSuchElementException();
            return datum;
        }
    }

    @Override
    public Iterator<T> iterator() {
        return new AdapterIterator();
    }
}
