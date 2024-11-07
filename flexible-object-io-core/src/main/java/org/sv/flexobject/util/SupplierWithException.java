package org.sv.flexobject.util;

public interface SupplierWithException<T, E extends Exception> {

    T get() throws E;
}
