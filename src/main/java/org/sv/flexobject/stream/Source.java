package org.sv.flexobject.stream;

import java.util.stream.Stream;

public interface Source<T>{

    T get() throws Exception;

    boolean isEOF();

    default void setEOF() {}

    default void ack(){}

    default void close() throws Exception {}

    Stream<T> stream();
}
