package org.sv.flexobject.arrow.write;

import org.sv.flexobject.schema.SchemaException;
import org.apache.arrow.vector.ValueVector;

public class StructListVectorWriter extends ListVectorWriter{

    ArrowStructWriter structWriter;

    public StructListVectorWriter(String fieldName, ValueVector vector, ArrowStructWriter structWriter) {
        super(fieldName, vector);
        this.structWriter = structWriter;
    }

    @Override
    protected void writeValue(int position, Object value) {
        try {
            if (value == null)
                structWriter.setNull(position);
            else
                structWriter.write(position, value);
        } catch (Exception e) {
            throw new SchemaException("Failed to set field " + fieldName, e );
        }
    }

    @Override
    public void newBatch() {
        structWriter.newBatch();
    }
}
