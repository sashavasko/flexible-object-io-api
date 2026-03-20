package org.sv.flexobject.arrow.read;

import com.carfax.dt.streaming.Streamable;
import com.carfax.dt.streaming.schema.Schema;
import com.carfax.dt.streaming.schema.SchemaException;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.List;

public class ArrowStructReader extends ArrowRecordReader {
    StructVectorReader vectorReader;

    public ArrowStructReader(Class<? extends Streamable> schemaClass, List<Field> fields, String fieldName, ValueVector vector) {
        this(schemaClass, fields);
        vectorReader = new StructVectorReader(fieldName, vector);
        try {
            buildFieldReaders();
        } catch (NoSuchFieldException e) {
            throw new SchemaException("Failed to build Arrow readers for subfields of `" + fieldName + "` class " + schemaClass, e);
        }
    }

    public ArrowStructReader(Class<? extends Streamable> schemaClass, List<Field> fields) {
        super(schemaClass, fields, Schema.getRegisteredSchema(schemaClass));
    }

    @Override
    protected FieldVector getFieldVector(String fieldName) {
        return vectorReader.getStructVector().getChild(fieldName);
    }

    @Override
    protected Streamable readRecordImpl(Streamable record, int recordIdx) {
        super.readRecordImpl(record, recordIdx);
        vectorReader.end(recordIdx);
        return record;
    }

    @Override
    public int getRowCount() {
        return vectorReader.getVector().getValueCount();
    }

    @Override
    public boolean isNull(int rowIndex) {
        return vectorReader.isNull(rowIndex);
    }
}
