package org.sv.flexobject.arrow.read;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.ListVector;

import java.util.ArrayList;
import java.util.List;

public abstract class ListVectorReader extends VectorReader {

    ListVector listVector;
    int currentPosition;

    public ListVectorReader(String fieldName, ValueVector vector) {
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
        return listVector.getElementStartIndex(rowIndex);
        //return listVector.getOffsetBuffer().getInt(((long)this.idx() + 1L) * 4L));
    }

    public int end(int rowIndex){
        int endPosition = listVector.getElementEndIndex(rowIndex);
        visitRow(rowIndex);
        return endPosition;
    }

    protected abstract Object readValue(int position);
    protected Object readValueAndIncrement(){
        Object value = readValue(currentPosition);
        currentPosition++;
        return value;
    }

    @Override
    public Object read(int rowIndex) {
        int startPosition = start(rowIndex);
        int endPosition = end(rowIndex);
        currentPosition = startPosition;
        List result = new ArrayList();
        while (currentPosition < endPosition){
            Object value = readValueAndIncrement();
            result.add(value);
        }
        visitRow(rowIndex);
        return result;
    }
}
