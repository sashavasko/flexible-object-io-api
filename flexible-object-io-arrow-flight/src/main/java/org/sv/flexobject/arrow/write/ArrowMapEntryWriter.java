package org.sv.flexobject.arrow.write;

import org.sv.flexobject.arrow.ArrowMapEntry;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.schema.FieldDescriptor;
import org.sv.flexobject.schema.SchemaException;
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
