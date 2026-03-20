package org.sv.flexobject.arrow.write;

import com.carfax.arrow.ArrowMapEntry;
import com.carfax.arrow.ArrowSchema;
import com.carfax.arrow.vector.VectorSetters;
import com.carfax.dt.streaming.Streamable;
import com.carfax.dt.streaming.schema.FieldDescriptor;
import com.carfax.dt.streaming.schema.Schema;
import com.carfax.dt.streaming.schema.SchemaException;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class ArrowRecordWriter implements ArrowWriter {
    Class <? extends Streamable> schemaClass;
    List<Field> fields;
    com.carfax.dt.streaming.schema.Schema internalSchema;
    int rowCount = 0;

    protected Map<String, ArrowWriter> fieldWriters = new HashMap<>();

    protected ArrowRecordWriter() {

    }

    protected ArrowRecordWriter(Class<? extends Streamable> schemaClass, List<Field> fields, com.carfax.dt.streaming.schema.Schema internalSchema) {
        setSchemaClass(schemaClass);
        setFields(fields);
        setInternalSchema(internalSchema);
    }

    protected void setSchemaClass(Class<? extends Streamable> schemaClass) {
        this.schemaClass = schemaClass;
    }

    protected void setFields(List<Field> fields) {
        this.fields = fields;
    }

    protected void setInternalSchema(Schema internalSchema) {
        this.internalSchema = internalSchema;
    }

    protected void writeRecordImpl(Streamable record, int recordIdx){
        for (Field field : fields) {
            String fieldName = field.getName();
            ArrowWriter writer = fieldWriters.get(fieldName);
            if (writer != null) {
                Object value = null;
                try {
                    value = record.get(fieldName);
                } catch (Exception e) {
                    throw new SchemaException("failed to get value for field " + fieldName, e);
                }
                if (value != null) {
                    writer.write(recordIdx, value);
                } else
                    writer.setNull(recordIdx);
            }
        }
    }

    protected void commitRow(){
        rowCount++;
    }

    public boolean writeRecord(Streamable record){
        if (record == null)
            setNull(rowCount);
        else
            writeRecordImpl(record, rowCount);
        commitRow();
        return true;
    }

    @Override
    public void write(int rowIndex, Object datum){
        Streamable record = null;
        if (datum != null) {
            if (datum instanceof Streamable)
                record = (Streamable) datum;
            else if (datum instanceof Map.Entry)
                record = new ArrowMapEntry((Map.Entry) datum);
            else
                throw new SchemaException("unsupported datum type: " + datum.getClass() + " - must be either Streamable or Map.Entry");
        }
        writeRecord(record);
    }

    public int getRowCount() {
        return rowCount;
    }

    @Override
    public void newBatch() {
        for (ArrowWriter writer : fieldWriters.values()) {
            writer.newBatch();
        }
        rowCount = 0;
    }

    public Class<? extends Streamable> getSchemaClass() {
        return schemaClass;
    }

    public List<Field> getFields() {
        return fields;
    }

    public com.carfax.dt.streaming.schema.Schema getInternalSchema() {
        return internalSchema;
    }

    protected abstract FieldVector getFieldVector(String fieldName);

    protected void buildFieldWriters() throws NoSuchFieldException {
        for (Field field : fields) {
            String fieldName = field.getName();
            FieldVector vector = getFieldVector(fieldName);
            FieldDescriptor descriptor = internalSchema.getDescriptor(fieldName);
            VectorSetters.Setter setter = VectorSetters.getSetter(vector, descriptor);
            if (setter != null) {
                addFieldWriter(fieldName, new ValueVectorWriter(fieldName, vector, setter));
            } else if (ArrowSchema.isList(field)) {
                Field elementField = field.getChildren().get(0);
                if (elementField == null)
                    throw new SchemaException("Vector for field " + fieldName + " has no element type defined");

                if (ArrowSchema.isSimple(elementField)) {
                    if (vector instanceof ListVector)
                        setter = VectorSetters.getSetter(((ListVector) vector).getDataVector(), descriptor);
                    else
                        setter = VectorSetters.getSetter(elementField, descriptor);
                    addFieldWriter(fieldName, new ValueListVectorWriter(fieldName, vector, setter));
                } else {
                    if (descriptor.getSubschema() == null)
                        throw new SchemaException("Vector for field " + fieldName + " is a Struct Vector, but schema is unknown");

                    ArrowStructWriter structWriter = new ArrowStructWriter(descriptor.getSubschema(), elementField.getChildren(), fieldName, ((ListVector) vector).getDataVector());
                    addFieldWriter(fieldName, new StructListVectorWriter(fieldName, vector, structWriter));
                }
            } else if (ArrowSchema.isMap(field)) {
                Field elementField = field.getChildren().get(0);
                if (elementField == null
                        || elementField.getChildren() == null
                        || elementField.getChildren().size() != 2)
                    throw new SchemaException("Vector for map field " + fieldName + " has no element type defined (should be key/value pair)");

                MapVectorWriter writer;
                if (descriptor.getSubschema() != null)
                    writer = new MapVectorWriter(fieldName, vector, elementField, descriptor.getSubschema());
                else
                    writer = new MapVectorWriter(fieldName, vector, elementField);


                addFieldWriter(fieldName, writer);

            } else if (ArrowSchema.isStruct(field)) {
                addFieldWriter(fieldName, buildStructFieldWriter(field, vector, descriptor));
            }
        }
    }

    protected Class <? extends Streamable> getFieldSubSchema(Field field, FieldDescriptor descriptor) throws NoSuchFieldException {
        return descriptor.getSubschema();
    }

    protected ArrowWriter buildStructFieldWriter(Field field, FieldVector vector, FieldDescriptor descriptor) throws NoSuchFieldException {
        String fieldName = field.getName();
        Class <? extends Streamable> subSchema = getFieldSubSchema(field, descriptor);

        if (subSchema == null)
            throw new SchemaException("Vector for struct field '" + fieldName + "' is a Struct Vector, but field schema is unknown(" + descriptor + ")");

        return new ArrowStructWriter(subSchema, field.getChildren(), fieldName, vector);
    }

    private void addFieldWriter(String fieldName, ArrowWriter writer) {
        fieldWriters.put(fieldName, writer);
    }

    @Override
    public void commit() {
        for (Map.Entry<String, ArrowWriter> entry : fieldWriters.entrySet()) {
            entry.getValue().commit();
        }
    }

    @Override
    public void close() throws Exception {
        for (Map.Entry<String, ArrowWriter> entry : fieldWriters.entrySet()) {
            entry.getValue().close();
        }
    }
}
