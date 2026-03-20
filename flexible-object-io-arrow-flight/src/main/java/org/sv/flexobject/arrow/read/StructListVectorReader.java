package org.sv.flexobject.arrow.read;

import org.sv.flexobject.schema.SchemaException;
import org.apache.arrow.vector.ValueVector;

public class StructListVectorReader extends ListVectorReader {

    ArrowStructReader structReader;

    public StructListVectorReader(String fieldName, ValueVector vector, ArrowStructReader structReader) {
        super(fieldName, vector);
        this.structReader = structReader;
    }

    @Override
    protected Object readValue(int position) {
        try {
            return structReader.read(position);
        } catch (Exception e) {
            throw new SchemaException("Failed to get field " + fieldName, e );
        }
    }

    @Override
    public boolean isNull(int rowIndex) {
        return structReader.isNull(rowIndex);
    }
}
