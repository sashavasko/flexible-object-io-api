package org.sv.flexobject.arrow.write;

import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.ValueVector;

public abstract class VectorWriter implements ArrowWriter{

    String fieldName;
    ValueVector vector;
    int lastRowIndex = 0;

    public VectorWriter(String fieldName, ValueVector vector) {
        this.fieldName = fieldName;
        this.vector = vector;
    }

    protected int visitRow(int rowIndex){
        if (lastRowIndex < rowIndex)
            lastRowIndex = rowIndex;
        return rowIndex;
    }

    @Override
    public void newBatch() {
        lastRowIndex = 0;
    }

    @Override
    public void setNull(int rowIndex) {
        if (vector instanceof BaseFixedWidthVector)
            ((BaseFixedWidthVector) vector).setNull(rowIndex);
        visitRow(rowIndex);
    }

    @Override
    public void commit() {
        vector.setValueCount(lastRowIndex+1);
    }

    @Override
    public void close() throws Exception {
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
