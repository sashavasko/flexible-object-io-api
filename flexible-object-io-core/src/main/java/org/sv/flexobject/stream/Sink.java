package org.sv.flexobject.stream;

import java.util.Iterator;

public interface Sink<T> {

    /**
     * @param value
     * @return If sink is full returns true, otherwise false;
     * @throws Exception
     */
    boolean put(T value) throws Exception;

    /**
     * @param values
     * @return If sink is full returns true, otherwise false;
     * @throws Exception
     */
    default boolean put(Iterator<T> values) throws Exception{
        while (values.hasNext()) {
            if (put(values.next()))
                return true;
        }
        return false;
    }

    default void setEOF() {}

    boolean hasOutput();

    default T get(){return null;}
}
