package org.sv.flexobject.io.producers;

import org.sv.flexobject.Loadable;
import org.sv.flexobject.io.Producer;

import java.util.Collection;
import java.util.Iterator;

public class CollectionProducer<T extends Loadable> extends Producer<T> {

    Collection<T> collection;
    Iterator<T> iterator;

    public CollectionProducer(Collection<T> collection) {
        this.collection = collection;
        this.iterator = collection.iterator();
    }

    @Override
    public Iterator iterator() {
        return this.iterator;
    }

    @Override
    public T produce() {
        if (iterator.hasNext())
            return iterator.next();
        return null;
    }
}
