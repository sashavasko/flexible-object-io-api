package org.sv.flexobject.util;

public interface TriConsumerWithException<T1, T2, T3, E extends Exception> {

    void accept(T1 input1, T2 input2, T3 input3) throws E;

}
