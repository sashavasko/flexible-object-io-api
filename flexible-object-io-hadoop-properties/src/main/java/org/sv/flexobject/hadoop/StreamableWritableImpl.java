package org.sv.flexobject.hadoop;

import org.sv.flexobject.Streamable;

public class StreamableWritableImpl implements StreamableWritable{

    @Override
    public boolean equals(Object o) {
        return Streamable.equals(this, o);
    }

    @Override
    public String toString() {
        return Streamable.toString(this);
    }

}
