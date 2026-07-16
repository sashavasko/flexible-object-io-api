package org.sv.flexobject.arrow.read;

import org.apache.arrow.vector.ValueVector;

public abstract class VectorReader extends ArrowReader {

    String fieldName;
    ValueVector vector;
    int lastRowIndex = 0;

    public VectorReader(String fieldName, ValueVector vector) {
        this.fieldName = fieldName;
        this.vector = vector;
    }

    protected int visitRow(int rowIndex){
        if (lastRowIndex < rowIndex)
            lastRowIndex = rowIndex;
        return rowIndex;
    }

    public String getFieldName() {
        return fieldName;
    }

    public ValueVector getVector() {
        return vector;
    }

    public int getLastRowIndex() {
        return lastRowIndex;
    }
}
