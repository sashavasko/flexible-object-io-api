package org.sv.flexobject.arrow.vector;

import org.apache.arrow.vector.FieldVector;

public class VectorUtils {

    public static FieldVector copyVector(FieldVector source) {
        FieldVector copy = source.getField().createVector(source.getAllocator());
        copy.allocateNew();

        for(int i = 0; i < source.getValueCount(); ++i) {
            copy.copyFromSafe(i, i, source);
        }

        copy.setValueCount(source.getValueCount());
        return copy;
    }
}
