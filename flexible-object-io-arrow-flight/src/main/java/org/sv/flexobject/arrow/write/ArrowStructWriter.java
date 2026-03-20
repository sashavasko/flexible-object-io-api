package org.sv.flexobject.arrow.write;

import org.sv.flexobject.Streamable;
import org.sv.flexobject.schema.Schema;
import org.sv.flexobject.schema.SchemaException;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.List;

public class ArrowStructWriter extends ArrowRecordWriter{
    StructVectorWriter vectorWriter;

    public ArrowStructWriter(Class<? extends Streamable> schemaClass, List<Field> fields, String fieldName, ValueVector vector) {
        this(schemaClass, fields);
        vectorWriter = new StructVectorWriter(fieldName, vector);
        try {
            buildFieldWriters();
        } catch (NoSuchFieldException e) {
            throw new SchemaException("Failed to build Arrow writers for subfields of " + fieldName + " class " + schemaClass, e);
        }
    }

    public ArrowStructWriter(Class<? extends Streamable> schemaClass, List<Field> fields) {
        super(schemaClass, fields, Schema.getRegisteredSchema(schemaClass));
    }

    @Override
    protected FieldVector getFieldVector(String fieldName) {
        return vectorWriter.getStructVector().getChild(fieldName);
    }

    @Override
    protected void writeRecordImpl(Streamable record, int recordIdx) {
        vectorWriter.start(recordIdx);
        super.writeRecordImpl(record, recordIdx);
        vectorWriter.end(recordIdx);
    }

    @Override
    public void setNull(int rowIndex) {
        vectorWriter.setNull(rowIndex);
        vectorWriter.end(rowIndex);
        commitRow();
    }

    @Override
    public void close() throws Exception {

    }
}
