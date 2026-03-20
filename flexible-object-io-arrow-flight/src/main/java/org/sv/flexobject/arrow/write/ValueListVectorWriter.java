package org.sv.flexobject.arrow.write;

import org.sv.flexobject.arrow.vector.VectorSetters;
import org.sv.flexobject.schema.SchemaException;
import org.apache.arrow.vector.ValueVector;

public class ValueListVectorWriter extends ListVectorWriter{

    VectorSetters.Setter setter;

    public ValueListVectorWriter(String fieldName, ValueVector vector, VectorSetters.Setter setter) {
        super(fieldName, vector);
        this.setter = setter;
    }

    @Override
    protected void writeValue(int position, Object value) {
        try {
            if (value == null)
                getFieldVector().setNull(position);
            else
                setter.accept(getFieldVector(), position, value);
        } catch (Exception e) {
            throw new SchemaException("Failed to set field " + fieldName, e );
        }
    }
}
