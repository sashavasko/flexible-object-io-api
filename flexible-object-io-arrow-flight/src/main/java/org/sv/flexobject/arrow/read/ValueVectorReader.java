package org.sv.flexobject.arrow.read;

import com.carfax.arrow.vector.VectorGetters;
import com.carfax.dt.streaming.schema.SchemaException;
import org.apache.arrow.vector.ValueVector;

public class ValueVectorReader extends VectorReader {

    VectorGetters.Getter getter;

    public ValueVectorReader(String fieldName, ValueVector vector, VectorGetters.Getter getter) {
        super(fieldName, vector);
        this.getter = getter;
    }

    @Override
    public boolean isNull(int rowIndex) {
        return vector.isNull(rowIndex);
    }

    @Override
    public Object read(int rowIndex) {
        try {
            visitRow(rowIndex);
            return isNull(rowIndex) ? null : getter.get(vector, rowIndex);

        } catch (Exception e) {
            throw new SchemaException("Failed to get field " + fieldName, e );
        }
    }
}
