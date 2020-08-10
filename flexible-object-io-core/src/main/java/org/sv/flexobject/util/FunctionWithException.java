package org.sv.flexobject.util;

public interface FunctionWithException<T, R, E extends Exception> {

    R apply(T input) throws E;

}
