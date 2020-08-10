package org.sv.flexobject.io.producers;

import org.sv.flexobject.Loadable;
import org.sv.flexobject.io.Producer;

import java.util.Iterator;

public class SingleProducer extends Producer {

    Loadable datum;

    public SingleProducer(Loadable datum) {
        this.datum = datum;
    }

    @Override
    public Iterator<Loadable> iterator() {
        return new Iterator<Loadable>() {
            boolean done = false;

            @Override
            public boolean hasNext() {
                return !done;
            }

            @Override
            public Loadable next() {
                done = true;
                return datum;
            }
        };
    }

    @Override
    public Loadable produce() {
        Loadable toReturn = datum;
        datum = null;
        return toReturn;
    }
}
