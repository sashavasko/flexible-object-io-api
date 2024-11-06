package org.sv.flexobject.io.producers;

import org.sv.flexobject.Loadable;
import org.sv.flexobject.io.CloseableProducer;
import org.sv.flexobject.stream.Source;

import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class ConvertingSourceProducer<ST extends Loadable, DT extends Loadable> extends CloseableProducer<DT> {
    Source<ST> source;

    public abstract DT convert(ST sourceDatum);

    public ConvertingSourceProducer(Source source) {
        this.source = source;
    }

    @Override
    public void unsafeClose() throws Exception {
        source.close();
    }

    public DT produce(){
        ST sourceDatum = null;

        try {
            sourceDatum = source.get();
            if (sourceDatum != null) {
                return convert(sourceDatum);
            }
        } catch (Exception e) {
            setException(e);
        }
        return null;
    }

    @Override
    public void ack() {
        source.ack();
    }

    @Override
    public void setEOF() {
        source.setEOF();
    }

    public class SourceIterator implements Iterator<DT>{

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
    public Iterator iterator() {
        return new SourceIterator();
    }

}
