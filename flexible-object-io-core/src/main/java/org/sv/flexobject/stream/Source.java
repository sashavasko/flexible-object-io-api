package org.sv.flexobject.stream;

import java.util.Iterator;
import java.util.stream.Stream;

/**
 * Source produces complete objects of uniform type/class with possibly complex substructure.
 * It is different from InAdapter in a way that it does not provide access to subfields of objects being produced.
 *
 * Source can use InAdapter to convert flat-structured data, such as cvs file or a SQL RecordSet to a complex objects using Readers or load method of Streamable or StreamableWithSchema.
 */
public interface Source<T> extends AutoCloseable, Iterable<T>{

    <O extends T> O get() throws Exception;

    boolean isEOF();

    default void setEOF() {}

    default void ack(){}

    @Override
    default void close() throws Exception {setEOF();}

    Stream<T> stream();

    @Override
    default Iterator<T> iterator(){
        return new SourceIterator(this);
    }
}
