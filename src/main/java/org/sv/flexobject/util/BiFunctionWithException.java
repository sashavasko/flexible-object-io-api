package org.sv.flexobject.util;

public interface BiFunctionWithException<T1, T2, R, E extends Exception> {

    R apply(T1 input1, T2 input2) throws E;

}
