package org.sv.flexobject.io.producers;


import org.sv.flexobject.Loadable;
import org.sv.flexobject.stream.Source;

public class SourceProducer<T extends Loadable> extends ConvertingSourceProducer<T, T> {

    public SourceProducer(Source source) {
        super(source);
    }

    @Override
    public T convert(T sourceDatum) {
        return sourceDatum;
    }
}
