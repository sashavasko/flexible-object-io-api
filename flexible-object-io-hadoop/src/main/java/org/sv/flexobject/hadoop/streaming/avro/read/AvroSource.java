package org.sv.flexobject.hadoop.streaming.avro.read;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.hadoop.streaming.avro.AvroSchema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AvroSource<T extends StreamableWithSchema> extends AvroGenericSource<GenericRecord, T> {

    Class<? extends StreamableWithSchema> schema;

    public AvroSource(Class<? extends StreamableWithSchema> schema) {
        this.schema = schema;
        builder().withSchema(schema);
    }

    @Override
    protected T unwrap(GenericRecord value) {
        try {
            T convertedValue = (T) convertGenericRecord(value, value.getSchema());
            return convertedValue;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static StreamableWithSchema convertGenericRecord(GenericRecord src, Schema avroSchema) throws Exception {
        Class<? extends StreamableWithSchema> dstClass = (Class<? extends StreamableWithSchema>) Class.forName(avroSchema.getFullName());
        StreamableWithSchema dst = dstClass.newInstance();
        for (Schema.Field field : avroSchema.getFields()) {
            Schema recordSchema = AvroSchema.findRecordSchema(field);
            Object value = src.get(field.name());
            if (value == null){
                dst.set(field.name(), null);
            } else {
                if (recordSchema != null) {
                    dst.set(field.name(), convertGenericRecord((GenericRecord) value, recordSchema));
                } else if (value instanceof GenericData.Array) {
                    recordSchema = AvroSchema.findRecordSchema(AvroSchema.findArraySchema(field).getElementType());
                    dst.set(field.name(), convertAvroArray((GenericData.Array) value, recordSchema));
                } else if (value instanceof List) {
                    recordSchema = AvroSchema.findRecordSchema(AvroSchema.findArraySchema(field).getElementType());
                    dst.set(field.name(), convertAvroList((List) value, recordSchema));
                } else if (value instanceof Map) {
                    recordSchema = AvroSchema.findRecordSchema(AvroSchema.findMapSchema(field).getValueType());
                    dst.set(field.name(), convertAvroMap((Map) value, recordSchema));
                } else
                    dst.set(field.name(), convertAvroValue(value, null));
            }
        }
        return dst;
    }

    public static Map convertAvroMap(Map<Utf8, Object> avroMap, Schema recordSchema) throws Exception {
        Map convertedMap = new HashMap<String, Object>();
        for (Map.Entry<Utf8,Object> entry : avroMap.entrySet()){
            convertedMap.put(convertAvroValue(entry.getKey(), null), convertAvroValue(entry.getValue(), recordSchema));
        }
        return convertedMap;
    }

    public static List convertAvroList(List avroList, Schema recordSchema) throws Exception {
        List convertedList = new ArrayList(avroList.size());
        for (Object item : avroList)
            convertedList.add(convertAvroValue(item, recordSchema));
        return convertedList;
    }

    public static Object[] convertAvroArray(GenericData.Array avroArray, Schema recordSchema) throws Exception {
        Object[] convertedArray = new Object[avroArray.size()];
        for (int i = 0 ; i < avroArray.size() ; ++i){
            convertedArray[i] = convertAvroValue(avroArray.get(i), recordSchema);
        }
        return convertedArray;
    }

    public static Object convertAvroValue(Object value, Schema recordSchema) throws Exception {
        if (value == null)
            return null;

        if (value instanceof Utf8)
            return value.toString();

        if (value instanceof GenericData.Record)
            return convertGenericRecord((GenericRecord) value, recordSchema);

        return value;
    }
}
