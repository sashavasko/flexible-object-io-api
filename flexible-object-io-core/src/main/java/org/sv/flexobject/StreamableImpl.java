package org.sv.flexobject;

/**
 * Simple implementation of Streamable adding toString() and equals() methods.
 */
public class StreamableImpl implements Streamable {

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Streamable other = (Streamable) o;
        if (!getSchema().equals(other.getSchema()))
            return false;

        return getSchema().compareFields(this, other);
    }

    @Override
    public String toString() {
        try {
            return toJson().toString();
        } catch (Exception e) {
            return super.toString();
        }
    }

}
