package org.sv.flexobject.arrow.write;

import com.carfax.arrow.vector.VectorSetters;
import com.carfax.dt.streaming.schema.SchemaException;
import org.apache.arrow.vector.ValueVector;

public class ValueVectorWriter extends VectorWriter{

    VectorSetters.Setter setter;

    public ValueVectorWriter(String fieldName, ValueVector vector, VectorSetters.Setter setter) {
        super(fieldName, vector);
        this.setter = setter;
    }

    @Override
    public void write(int rowIndex, Object datum) {
        try {
            setter.accept(vector, visitRow(rowIndex), datum);
        } catch (Exception e) {
            throw new SchemaException("Failed to set field " + fieldName, e );
        }
    }
}
