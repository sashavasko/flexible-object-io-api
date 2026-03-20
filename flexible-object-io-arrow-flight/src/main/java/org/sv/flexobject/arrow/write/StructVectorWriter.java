package org.sv.flexobject.arrow.write;

import com.carfax.dt.streaming.schema.SchemaException;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.complex.StructVector;

public class StructVectorWriter extends VectorWriter{

    StructVector structVector;

    public StructVectorWriter(String fieldName, ValueVector vector) {
        super(fieldName, vector);
        this.structVector = (StructVector) vector;
    }

    public StructVector getStructVector() {
        return structVector;
    }

    public void start(int rowIndex){
        structVector.setIndexDefined(rowIndex);
//        structVector.addOrGetStruct(fieldName);
    }

    public void end(int rowIndex){
        visitRow(rowIndex);
    }

    @Override
    public void setNull(int rowIndex) {
        structVector.setNull(rowIndex);
    }

    @Override
    public void write(int rowIndex, Object datum) {
        throw new SchemaException("Operation not supported - use ArrowStructWriter");
    }
}
