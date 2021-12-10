package org.sv.flexobject;

/**
 * Simple implementation of Streamable adding toString() and equals() methods.
 */
public class StreamableImpl implements Streamable {

    @Override
    public boolean equals(Object o) {
        return Streamable.equals(this, o);
    }

    @Override
    public String toString() {
        return Streamable.toString(this);
    }
}
