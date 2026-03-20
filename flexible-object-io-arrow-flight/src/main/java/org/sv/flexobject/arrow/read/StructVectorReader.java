package org.sv.flexobject.arrow.read;

import org.sv.flexobject.schema.SchemaException;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.StructVector;

public class StructVectorReader extends VectorReader {

    StructVector structVector;

    public StructVectorReader(String fieldName, ValueVector vector) {
        super(fieldName, vector);
        this.structVector = (StructVector) vector;
    }

    public StructVector getStructVector() {
        return structVector;
    }

//    public void start(int rowIndex){
//        structVector.is.setIndexDefined(rowIndex);
//    }

    public void end(int rowIndex){
        visitRow(rowIndex);
    }

    @Override
    public boolean isNull(int rowIndex) {
        return structVector.isNull(rowIndex);
    }

    @Override
    public Object read(int rowIndex) {
        throw new SchemaException("Operation not supported - use ArrowStructReader");
    }
}
