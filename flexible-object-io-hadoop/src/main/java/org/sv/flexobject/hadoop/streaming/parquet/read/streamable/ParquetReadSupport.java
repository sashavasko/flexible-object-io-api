package org.sv.flexobject.hadoop.streaming.parquet.read.streamable;

import org.apache.parquet.schema.MessageType;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.hadoop.streaming.parquet.ParquetSchema;
import org.sv.flexobject.hadoop.streaming.parquet.read.SchemedGroupConverter;
import org.sv.flexobject.hadoop.streaming.parquet.read.SchemedReadSupport;
import org.sv.flexobject.schema.FieldDescriptor;
import org.sv.flexobject.schema.reflect.FieldWrapper;

import java.util.Collection;

public class ParquetReadSupport extends SchemedReadSupport<Streamable> {

    public ParquetReadSupport() {
    }

    public ParquetReadSupport(MessageType schema) {
        super(schema);
    }

    @Override
    public SchemedGroupConverter<Streamable> newGroupConverter(MessageType schema, MessageType fileSchema) {
        Class<?> datumClass = ParquetSchema.forType(schema);
        if (Streamable.class.isAssignableFrom(datumClass))
            return new StreamableConverter(schema, fileSchema, (Class<? extends Streamable>)datumClass);
        else
            throw new RuntimeException("Unsupported datum class " + datumClass.getName());
    }


    protected static void setFieldValue(Streamable current, String name, Object value) throws Exception {
        FieldDescriptor descriptor = current.getSchema().getDescriptor(name);
        if (descriptor.getStructure() == FieldWrapper.STRUCT.list){
            Collection list = (Collection)current.get(name);
            list.add(value);
        } else
            current.set(name, value);
    }

    protected static void setField(Streamable current, String name, Object value) {
        try {
            setFieldValue(current, name, value);
        } catch (Exception e) {
            if (e instanceof RuntimeException)
                throw (RuntimeException) e;
            throw new RuntimeException("Failed to set value for " + name + " in " + current.getClass().getSimpleName(), e);
        }
    }

}
