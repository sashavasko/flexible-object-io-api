package org.sv.flexobject.arrow.write;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.ListVector;

import java.util.Collection;

public abstract class ListVectorWriter extends VectorWriter{

    ListVector listVector;
    int currentPosition;

    public ListVectorWriter(String fieldName, ValueVector vector) {
        super(fieldName, vector);
        this.listVector = (ListVector) vector;
    }

    public ListVector getListVector() {
        return listVector;
    }

    public FieldVector getFieldVector() {
        return listVector.getDataVector();
    }

    public int start(int rowIndex){
        return listVector.startNewValue(rowIndex);
        //return listVector.getOffsetBuffer().getInt(((long)this.idx() + 1L) * 4L));
    }

    public void end(int rowIndex, int size){
        listVector.endValue(rowIndex, size);
        visitRow(rowIndex);
    }

    protected abstract void writeValue(int position, Object value);
    protected void writeValueAndIncrement(Object value){
        writeValue(currentPosition, value);
        currentPosition++;
    }

    @Override
    public void setNull(int rowIndex) {
        listVector.setNull(rowIndex);
    }

    @Override
    public void write(int rowIndex, Object datum) {
        int startPosition = start(rowIndex);
        currentPosition = startPosition;
        if (datum instanceof Collection){
            ((Collection)datum).forEach(this::writeValueAndIncrement);
        } else if (datum instanceof Object[]){
            for (Object o : (Object[])datum)
                writeValueAndIncrement(o);
        } else if (datum instanceof int[]){
            for (int o : (int[])datum)
                writeValueAndIncrement(o);
        } else if (datum instanceof long[]){
            for (long o : (long[])datum)
                writeValueAndIncrement(o);
        } else if (datum instanceof float[]){
            for (float o : (float[])datum)
                writeValueAndIncrement(o);
        } else if (datum instanceof double[]){
            for (double o : (double[])datum)
                writeValueAndIncrement(o);
        } else if (datum instanceof boolean[]){
            for (boolean o : (boolean[])datum)
                writeValueAndIncrement(o);
        }
        end(rowIndex, currentPosition - startPosition);
    }
}
