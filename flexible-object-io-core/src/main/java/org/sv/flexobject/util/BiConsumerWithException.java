package org.sv.flexobject.util;

public interface BiConsumerWithException<T1, T2, E extends Exception> {

    void accept(T1 input1, T2 input2) throws E;

}
