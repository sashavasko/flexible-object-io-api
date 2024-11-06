package org.sv.flexobject.io.producers;


import org.sv.flexobject.InAdapter;
import org.sv.flexobject.Loadable;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.adapter.GenericInAdapter;
import org.sv.flexobject.io.CloseableProducer;
import org.sv.flexobject.io.Reader;
import org.sv.flexobject.schema.Schema;
import org.sv.flexobject.stream.Source;
import org.sv.flexobject.util.InstanceFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;

public abstract class ConvertingAdapterProducer<ST extends Loadable, DT extends Loadable> extends CloseableProducer<DT> {
    InAdapter adapter;
    Reader<ST> reader;

    public abstract DT convert(ST sourceDatum);

    public ConvertingAdapterProducer() {
    }

    @Deprecated  // Use builder() instead
    public ConvertingAdapterProducer(Source source, Class<? extends GenericInAdapter> adapterClass, Reader reader) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        adapter = GenericInAdapter.build(adapterClass, source);
        this.reader = reader;
    }

    @Deprecated  // Use builder() instead
    public ConvertingAdapterProducer(InAdapter adapter, Reader reader) {
        this.adapter = adapter;
        this.reader = reader;
    }

    public static class Builder<A extends ConvertingAdapterProducer> {
        Source source;
        Class<? extends GenericInAdapter> adapterClass;
        Reader reader;
        InAdapter adapter;
        Class<? extends ConvertingAdapterProducer> producerClass = AdapterProducer.class;
        Function converter;

        protected Builder() {
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

        public Builder forClass(Class<? extends Streamable> streamableClass){
            this.reader = Schema.getRegisteredSchema(streamableClass).getReader();
            return this;
        }

        public Builder instanceOf(Class<? extends A> producerClass){
            this.producerClass = producerClass;
            return this;
        }

        public A build() {
            A producer = (A) InstanceFactory.get(producerClass);
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
        return new Builder<>();
    }

    @Deprecated  // Use builder() instead
    public ConvertingAdapterProducer(Source source, Class<? extends GenericInAdapter> adapterClass, Class dataClass) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        this(source, adapterClass, Schema.getRegisteredSchema(dataClass).getReader());
    }

    @Deprecated  // Use builder() instead
    public ConvertingAdapterProducer(InAdapter adapter, Class dataClass) {
        this(adapter, Schema.getRegisteredSchema(dataClass).getReader());
    }

    public void setAdapter(InAdapter adapter) {
        this.adapter = adapter;
    }

    public void setReader(Reader<ST> reader) {
        this.reader = reader;
    }

    @Override
    public void unsafeClose() throws Exception {
        adapter.close();
    }

    public DT produce(){
        ST datum = null;

        try {
            if (adapter.next()) {
                datum = reader.create();
                reader.convert(adapter, datum);
            }
        } catch (Exception e) {
            setException(e);
        }
        return datum == null ? null : convert(datum);
    }

    @Override
    public void ack() {
        adapter.ack();
    }

    @Override
    public void setEOF() {
        adapter.setEOF();
    }

    public class AdapterIterator implements Iterator<DT>{

        DT datum = null;

        @Override
        public boolean hasNext() {
            datum = produce();
            return datum != null;
        }

        @Override
        public DT next() {
            if (datum == null)
                throw new NoSuchElementException();
            return datum;
        }
    }

    @Override
    public Iterator<DT> iterator() {
        return new AdapterIterator();
    }

}
