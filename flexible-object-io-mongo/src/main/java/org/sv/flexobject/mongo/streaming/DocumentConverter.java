package org.sv.flexobject.mongo.streaming;

import org.bson.Document;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.schema.FieldDescriptor;
import org.sv.flexobject.schema.Schema;
import org.sv.flexobject.schema.SchemaElement;

import java.util.ArrayList;
import java.util.List;

public class DocumentConverter {

    public static Streamable convertDocument(Document src, Class<? extends Streamable> dstClass) throws Exception {
        Streamable dst = dstClass.newInstance();
        Schema internalSchema = Schema.getRegisteredSchema(dstClass);
        for (SchemaElement field : internalSchema.getFields()) {
            String fieldName = field.getDescriptor().getName();
            FieldDescriptor descriptor = (FieldDescriptor) field.getDescriptor();
            Class<? extends Streamable> recordSchema = descriptor.getSubschema();
            Object value = src.get(fieldName);
            DataTypes valueType = descriptor.getValueType();
            if (value == null){
                dst.set(fieldName, null);
            } else {
                if (value instanceof List) {
                    dst.set(fieldName, convertList((List) value, recordSchema, valueType));
                } else if (recordSchema != null) {
                    dst.set(fieldName, convertDocument((Document) value, recordSchema));
                } else
                    dst.set(fieldName, convertBsonValue(value, null));
            }
        }
        return dst;
    }

    public static List convertList(List avroList, Class<? extends Streamable> recordSchema, DataTypes valueType) throws Exception {
        List convertedList = new ArrayList(avroList.size());
        for (Object item : avroList)
            convertedList.add(convertBsonValue(item, recordSchema, valueType));
        return convertedList;
    }

    public static Object convertBsonValue(Object value, Class<? extends Streamable>  recordSchema) throws Exception {
        return convertBsonValue(value, recordSchema, null);
    }

    public static Object convertBsonValue(Object value, Class<? extends Streamable>  recordSchema, DataTypes valueType) throws Exception {
        if (value == null)
            return null;

        if (value instanceof Document)
            return convertDocument((Document) value, recordSchema);

        return valueType == null ? value : valueType.convert(value);
    }


}
