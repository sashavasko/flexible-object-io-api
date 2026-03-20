package org.sv.flexobject.arrow.write;

import com.carfax.arrow.ArrowMapEntry;
import com.carfax.dt.streaming.Streamable;
import com.carfax.dt.streaming.schema.FieldDescriptor;
import com.carfax.dt.streaming.schema.SchemaException;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;

import java.util.List;

public class ArrowMapEntryWriter extends ArrowStructWriter{

    Class<? extends Streamable> valueSchemaClass;

    public ArrowMapEntryWriter(List<Field> fields, String fieldName, ValueVector vector, Class<? extends Streamable> valueSchemaClass) {
        super(ArrowMapEntry.class, fields);
        vectorWriter = new StructVectorWriter(fieldName, vector);
        this.valueSchemaClass = valueSchemaClass;
        try {
            buildFieldWriters();
        } catch (NoSuchFieldException e) {
            throw new SchemaException("Failed to build Arrow writers for subfields of " + fieldName + " class " + schemaClass, e);
        }
    }

    @Override
    protected Class<? extends Streamable> getFieldSubSchema(Field field, FieldDescriptor descriptor) throws NoSuchFieldException {
        Class <? extends Streamable> subSchema = super.getFieldSubSchema(field, descriptor);
        if (subSchema == null && "value".equals(field.getName())){
            subSchema = valueSchemaClass;
        }
        return subSchema;
    }
}
