package org.sv.flexobject.stream;

public interface Sink<T> {

    boolean put(T value) throws Exception;

    default void setEOF() {}

    boolean hasOutput();

    default T get(){return null;}
}
