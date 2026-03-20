package org.sv.flexobject.arrow.read;

import com.carfax.arrow.vector.VectorGetters;
import com.carfax.dt.streaming.schema.SchemaException;
import org.apache.arrow.vector.ValueVector;

public class ValueListVectorReader extends ListVectorReader {

    VectorGetters.Getter getter;

    public ValueListVectorReader(String fieldName, ValueVector vector, VectorGetters.Getter getter) {
        super(fieldName, vector);
        this.getter = getter;
    }

    @Override
    protected Object readValue(int position) {
        try {
            return getter.get(getFieldVector(), position);
        } catch (Exception e) {
            throw new SchemaException("Failed to get field " + fieldName, e );
        }
    }

    @Override
    public boolean isNull(int rowIndex) {
        return getFieldVector().isNull(rowIndex);
    }
}
