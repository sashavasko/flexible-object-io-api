package org.sv.flexobject.io.producers;

import org.sv.flexobject.Loadable;
import org.sv.flexobject.io.Producer;

import java.util.Collection;
import java.util.Iterator;

public class CollectionProducer extends Producer {

    Collection<Loadable> collection;
    Iterator<Loadable> iterator;

    public CollectionProducer(Collection<Loadable> collection) {
        this.collection = collection;
        this.iterator = collection.iterator();
    }

    @Override
    public Iterator iterator() {
        return this.iterator;
    }

    @Override
    public Loadable produce() {
        if (iterator.hasNext())
            return iterator.next();
        return null;
    }
}
