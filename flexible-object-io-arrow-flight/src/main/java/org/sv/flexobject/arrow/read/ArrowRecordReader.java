package org.sv.flexobject.arrow.read;

import com.carfax.arrow.ArrowSchema;
import com.carfax.arrow.vector.VectorGetters;
import com.carfax.dt.streaming.Streamable;
import com.carfax.dt.streaming.schema.FieldDescriptor;
import com.carfax.dt.streaming.schema.Schema;
import com.carfax.dt.streaming.schema.SchemaException;
import com.carfax.utility.InstanceFactory;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class ArrowRecordReader extends ArrowReader{

    public Class<? extends Streamable> getSchemaClass() {
        return schemaClass;
    }

    Class <? extends Streamable> schemaClass;
    List<Field> fields;
    com.carfax.dt.streaming.schema.Schema internalSchema;
    int rowIndex = 0;

    protected Map<String, ArrowReader> fieldReaders = new HashMap<>();

    public ArrowRecordReader() {
    }

    protected ArrowRecordReader(Class<? extends Streamable> schemaClass, List<Field> fields, com.carfax.dt.streaming.schema.Schema internalSchema) {
        setSchemaClass(schemaClass);
        setFields(fields);
        setInternalSchema(internalSchema);
    }

    public int getRowIndex() {
        return rowIndex;
    }



    public void setSchemaClass(Class<? extends Streamable> schemaClass) {
        this.schemaClass = schemaClass;
    }

    public void setFields(List<Field> fields) {
        this.fields = fields;
    }

    public void setInternalSchema(Schema internalSchema) {
        this.internalSchema = internalSchema;
    }

    protected Streamable readRecordImpl(Streamable record, int recordIdx){
        for (Field field : fields) {
            String fieldName = field.getName();
            ArrowReader reader = fieldReaders.get(fieldName);
            FieldDescriptor descriptor = internalSchema.getDescriptor(fieldName);
            if (descriptor != null && reader != null) {
                Object value = reader.read(recordIdx);

                try {
                    record.set(fieldName, value);
                } catch (Exception e) {
                    throw new SchemaException("failed to set value for field `" + fieldName + "`, valueClass = " + value.getClass() + "\nvalue = " + value, e);
                }
            }
        }
        return record;
    }

    public abstract int getRowCount();

    public <T extends Streamable> T readRecord(){
        T record = (T) read(rowIndex);
        rowIndex++;
        return record;
    }

    @Override
    public Object read(int rowIndex){
        if (isNull(rowIndex))
            return null;
        Streamable record = InstanceFactory.get(schemaClass);
        return readRecordImpl(record, rowIndex);
   }

    protected abstract FieldVector getFieldVector(String fieldName);

    protected void buildFieldReaders() throws NoSuchFieldException {
        for (Field field : fields) {
            String fieldName = field.getName();
            FieldVector vector = getFieldVector(fieldName);
            FieldDescriptor descriptor = internalSchema.getDescriptor(fieldName);
            VectorGetters.Getter getter = VectorGetters.getGetter(vector, descriptor);
            if (getter != null) {
                addFieldReader(fieldName, new ValueVectorReader(fieldName, vector, getter));
            } else if (ArrowSchema.isList(field)) {
                Field elementField = field.getChildren().get(0);
                if (elementField == null)
                    throw new SchemaException("Vector for field " + fieldName + " has no element type defined");

                if (ArrowSchema.isSimple(elementField)) {
                    if (vector instanceof ListVector)
                        getter = VectorGetters.getGetter(((ListVector) vector).getDataVector(), descriptor);
                    else
                        getter = VectorGetters.getGetter(elementField, descriptor);
                    addFieldReader(fieldName, new ValueListVectorReader(fieldName, vector, getter));
                } else {
                    if (descriptor.getSubschema() == null)
                        throw new SchemaException("Vector for field " + fieldName + " is a Struct Vector, but schema is unknown");

                    ArrowStructReader structReader = new ArrowStructReader(descriptor.getSubschema(), elementField.getChildren(), fieldName, ((ListVector) vector).getDataVector());
                    addFieldReader(fieldName, new StructListVectorReader(fieldName, vector, structReader));
                }
            } else if (ArrowSchema.isMap(field)) {
                Field elementField = field.getChildren().get(0);
                if (elementField == null
                        || elementField.getChildren() == null
                        || elementField.getChildren().size() != 2)
                    throw new SchemaException("Vector for map field " + fieldName + " has no element type defined (should be key/value pair)");

                MapVectorReader reader;
                if (descriptor.getSubschema() != null)
                    reader = new MapVectorReader(fieldName, vector, elementField, descriptor.getSubschema());
                else
                    reader = new MapVectorReader(fieldName, vector, elementField);


                addFieldReader(fieldName, reader);

            } else if (ArrowSchema.isStruct(field)) {
                addFieldReader(fieldName, buildStructFieldReader(field, vector, descriptor));
            }
        }
    }

    protected Class <? extends Streamable> getFieldSubSchema(Field field, FieldDescriptor descriptor) throws NoSuchFieldException {
        return descriptor.getSubschema();
    }

    protected ArrowReader buildStructFieldReader(Field field, FieldVector vector, FieldDescriptor descriptor) throws NoSuchFieldException {
        String fieldName = field.getName();
        Class <? extends Streamable> subSchema = getFieldSubSchema(field, descriptor);

        if (subSchema == null)
            throw new SchemaException("Vector for struct field '" + fieldName + "' is a Struct Vector, but field schema is unknown(" + descriptor + ")");

        return new ArrowStructReader(subSchema, field.getChildren(), fieldName, vector);
    }

    private void addFieldReader(String fieldName, ArrowReader reader) {
        reader.setDictionaryMap(dictionaryMap);
        fieldReaders.put(fieldName, reader);
    }

    public void reset(){
        rowIndex = 0;
    }

}
