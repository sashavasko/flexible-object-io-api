package org.sv.flexobject.arrow.read;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.omg.CORBA.portable.Streamable;
import org.sv.flexobject.arrow.ArrowMapEntry;
import org.sv.flexobject.schema.FieldDescriptor;
import org.sv.flexobject.schema.SchemaException;

import java.util.List;

public class ArrowMapEntryReader extends ArrowStructReader {

    Class<? extends Streamable> valueSchemaClass;

    public ArrowMapEntryReader(List<Field> fields, String fieldName, ValueVector vector, Class<? extends Streamable> valueSchemaClass) {
        super(ArrowMapEntry.class, fields);
        vectorReader = new StructVectorReader(fieldName, vector);
        this.valueSchemaClass = valueSchemaClass;
        try {
            buildFieldReaders();
        } catch (NoSuchFieldException e) {
            throw new SchemaException("Failed to build Arrow readers for subfields of " + fieldName + " class " + schemaClass, e);
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
