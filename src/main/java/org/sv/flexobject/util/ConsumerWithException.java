package org.sv.flexobject.util;

public interface ConsumerWithException<T, E extends Exception> {

    void accept(T input) throws E;

}
