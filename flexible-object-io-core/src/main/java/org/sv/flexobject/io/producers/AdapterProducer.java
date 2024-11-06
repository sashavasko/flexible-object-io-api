package org.sv.flexobject.io.producers;

import org.sv.flexobject.InAdapter;
import org.sv.flexobject.Loadable;
import org.sv.flexobject.Streamable;
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

public class AdapterProducer<T extends Loadable> extends ConvertingAdapterProducer<T, T> {

    @Override
    public T convert(T sourceDatum) {
        return sourceDatum;
    }

    public AdapterProducer() {
        super();
    }

    public static class Builder extends ConvertingAdapterProducer.Builder<AdapterProducer>{
        @Override
        public AdapterProducer build() {
            return super.build();
        }
    }

    public static Builder builder(){
        return new Builder();
    }

    @Deprecated
    public AdapterProducer(Source source, Class<? extends GenericInAdapter> adapterClass, Reader reader) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        super(source, adapterClass, reader);
    }

    @Deprecated
    public AdapterProducer(InAdapter adapter, Reader reader) {
        super(adapter, reader);
    }

    @Deprecated
    public AdapterProducer(Source source, Class<? extends GenericInAdapter> adapterClass, Class dataClass) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        super(source, adapterClass, dataClass);
    }

    @Deprecated
    public AdapterProducer(InAdapter adapter, Class dataClass) {
        super(adapter, dataClass);
    }
}
