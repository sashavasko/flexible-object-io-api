package org.sv.flexobject.stream;

public interface Sink<T> {

    /**
     * @param value
     * @return If sink is full returns true, otherwise false;
     * @throws Exception
     */
    boolean put(T value) throws Exception;

    default void setEOF() {}

    boolean hasOutput();

    default T get(){return null;}
}
