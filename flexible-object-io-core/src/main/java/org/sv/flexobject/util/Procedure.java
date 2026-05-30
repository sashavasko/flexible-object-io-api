package org.sv.flexobject.util;

public interface Procedure {

    void invoke();

    static Procedure doNothing() {
        return () -> {};
    }

}
