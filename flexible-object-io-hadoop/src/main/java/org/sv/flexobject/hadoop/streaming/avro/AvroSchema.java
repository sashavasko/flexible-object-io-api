package org.sv.flexobject.hadoop.streaming.avro;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.schema.FieldDescriptor;
import org.sv.flexobject.schema.SchemaElement;
import org.sv.flexobject.schema.SchemaException;
import org.sv.flexobject.schema.reflect.FieldWrapper;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AvroSchema {

    static {
        DataTypes.string.registerCustomConverter(Utf8.class, (v)->((Utf8)v).toString());
    }

    public static Configuration overrideBackwardsOptions(Configuration conf){
        conf.setBoolean("parquet.strict.typing", false);
        conf.setBoolean("parquet.avro.add-list-element-records", false);
        conf.setBoolean("parquet.avro.write-old-list-structure", false);
        return conf;
    }

    public static Schema forClass(Class<? extends Streamable> dataClass) {
        try {
            return forSchema(org.sv.flexobject.schema.Schema.getRegisteredSchema(dataClass));
        } catch (Exception e) {
            throw new RuntimeException("Failed to compile Avro Schema for " + dataClass.getName(), e);
        }
    }

    public static Schema forSchema(org.sv.flexobject.schema.Schema internalSchema) throws NoSuchFieldException, SchemaException {
        List<Schema.Field> fields = compileFields(internalSchema);
        return Schema.createRecord(internalSchema.getSimpleName(), null, internalSchema.getNamespace(), false, fields);
    }

    public static List<Schema.Field> compileFields(org.sv.flexobject.schema.Schema internalSchema) throws NoSuchFieldException, SchemaException {
        SchemaElement[] internalFields = internalSchema.getFields();
        List<Schema.Field> fields = new ArrayList<>(internalFields.length);
        for (SchemaElement field : internalFields){
            FieldDescriptor descriptor = (FieldDescriptor) field.getDescriptor();
            if (descriptor.getValueType() == DataTypes.binary){
                fields.add(new Schema.Field(descriptor.getName(), Schema.create(Schema.Type.BYTES)));
            } else {
                fields.add(new Schema.Field(descriptor.getName(), nullable(compileField(descriptor))));
            }
        }
        return fields;
    }

    public static Schema nullable(Schema schema){
        return Schema.createUnion(Schema.create(Schema.Type.NULL), schema);
    }

    public static Schema compileField(FieldDescriptor field) throws NoSuchFieldException, SchemaException {
        FieldWrapper.STRUCT structure = field.getStructure();
        Class<? extends Streamable> subSchema = field.getSubschema();
        switch(structure){
            case array:
                if (field.getValueType() == DataTypes.binary){
                    return Schema.create(Schema.Type.BYTES);
                }
            case list:
                return Schema.createArray(nullable(compileField(field, subSchema)));
            case map:
                return Schema.createMap(nullable(compileField(field, subSchema)));
            default:
                return compileField(field, subSchema);
        }
    }

    private static Schema compileField(FieldDescriptor field, Class<? extends Streamable> subSchema) throws NoSuchFieldException, SchemaException {
        if (subSchema != null)
            return forClass(subSchema);

        switch(field.getValueType()){
            case bool: return Schema.create(Schema.Type.BOOLEAN);
            case localDate:
            case date: return LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
            case int32: return Schema.create(Schema.Type.INT);
            case int64: return Schema.create(Schema.Type.LONG);
            case classObject:
            case jsonNode:
            case string: return Schema.create(Schema.Type.STRING);
            case float64: return Schema.create(Schema.Type.DOUBLE);
            case timestamp: return LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
            case binary: return Schema.create(Schema.Type.BYTES);
        }
        throw new SchemaException("Unknown datatype of field " + field.getName() + " class " + subSchema);
    }

    public static Schema findRecordSchema(Schema.Field field) {
        return findContainerSchema(field, Schema.Type.RECORD);
    }

    public static Schema findArraySchema(Schema.Field field) {
        return findContainerSchema(field, Schema.Type.ARRAY);
    }

    public static Schema findMapSchema(Schema.Field field) {
        return findContainerSchema(field, Schema.Type.MAP);
    }

    public static Schema findRecordSchema(Schema schema) {
        return findContainerSchema(schema, Schema.Type.RECORD);
    }

    public static Schema findArraySchema(Schema schema) {
        return findContainerSchema(schema, Schema.Type.ARRAY);
    }

    public static Schema findMapSchema(Schema schema) {
        return findContainerSchema(schema, Schema.Type.MAP);
    }

    public static Schema findContainerSchema(Schema.Field field, Schema.Type containerType) {
        return findContainerSchema(field.schema(), containerType);
    }
    public static Schema findContainerSchema(Schema schema, Schema.Type containerType) {
        if (schema.isUnion()) {
            for (Schema item : schema.getTypes()) {
                if (item.getType() == containerType)
                    return item;
            }
        }
        return null;
    }

    public static <T extends Streamable> T convertGenericRecord(GenericRecord src, Schema avroSchema) throws Exception {
        Class<? extends Streamable> dstClass = (Class<? extends Streamable>) Class.forName(avroSchema.getFullName());
        Streamable dst = dstClass.newInstance();
        return convertGenericRecord(src, avroSchema, dst);
    }

    public static <T extends Streamable> T convertGenericRecord(GenericRecord src, Schema avroSchema, Streamable dst) throws Exception {
        Class<? extends Streamable> dstClass = dst.getClass();
        org.sv.flexobject.schema.Schema internalSchema = dst.getSchema();
        for (Schema.Field field : avroSchema.getFields()) {
            Schema recordSchema = AvroSchema.findRecordSchema(field);
            Object value = src.get(field.name());
            DataTypes valueType = internalSchema.getDescriptor(field.name()).getValueType();
            if (value == null){
                dst.set(field.name(), null);
            } else {
                if (recordSchema != null) {
                    dst.set(field.name(), convertGenericRecord((GenericRecord) value, recordSchema));
                } else if (value instanceof GenericData.Array) {
                    recordSchema = AvroSchema.findRecordSchema(AvroSchema.findArraySchema(field).getElementType());
                    dst.set(field.name(), convertAvroArray((GenericData.Array) value, recordSchema, valueType));
                } else if (value instanceof List) {
                    recordSchema = AvroSchema.findRecordSchema(AvroSchema.findArraySchema(field).getElementType());
                    dst.set(field.name(), convertAvroList((List) value, recordSchema, valueType));
                } else if (value instanceof Map) {
                    recordSchema = AvroSchema.findRecordSchema(AvroSchema.findMapSchema(field).getValueType());
                    dst.set(field.name(), convertAvroMap((Map) value, recordSchema, valueType));
                } else if (field.schema().getType() == Schema.Type.BYTES) {
                    dst.set(field.name(), ((ByteBuffer)value).array());
                }else
                    dst.set(field.name(), convertAvroValue(value, null));
            }
        }
        return (T) dstClass.cast(dst);
    }

    public static Map convertAvroMap(Map<Utf8, Object> avroMap, Schema recordSchema, DataTypes valueType) throws Exception {
        Map convertedMap = new HashMap<String, Object>();
        for (Map.Entry<Utf8,Object> entry : avroMap.entrySet()){
            convertedMap.put(convertAvroValue(entry.getKey(), null), convertAvroValue(entry.getValue(), recordSchema, valueType));
        }
        return convertedMap;
    }

    public static List convertAvroList(List avroList, Schema recordSchema, DataTypes valueType) throws Exception {
        List convertedList = new ArrayList(avroList.size());
        for (Object item : avroList)
            convertedList.add(convertAvroValue(item, recordSchema, valueType));
        return convertedList;
    }

    public static Object[] convertAvroArray(GenericData.Array avroArray, Schema recordSchema, DataTypes valueType) throws Exception {
        Object[] convertedArray = new Object[avroArray.size()];
        for (int i = 0 ; i < avroArray.size() ; ++i){
            convertedArray[i] = convertAvroValue(avroArray.get(i), recordSchema, valueType);
        }
        return convertedArray;
    }

    public static Object convertAvroValue(Object value, Schema recordSchema) throws Exception {
        return convertAvroValue(value, recordSchema, null);
    }

    public static Object convertAvroValue(Object value, Schema recordSchema, DataTypes valueType) throws Exception {
        if (value == null)
            return null;

        if (value instanceof GenericData.Record)
            return convertGenericRecord((GenericRecord) value, recordSchema);

        if (value instanceof Utf8)
            value = value.toString();

        return valueType == null ? value : valueType.convert(value);
    }

    public static Object toAvro(FieldDescriptor descriptor, Object value) throws Exception {
        if (value == null)
            return null;

        FieldWrapper.STRUCT structure = descriptor.getStructure();

        Class<? extends Streamable> subSchema = descriptor.getSubschema();
        Schema avroSubSchema = subSchema == null ? null : AvroSchema.forClass(subSchema);

        switch (structure) {
            case array:
                List avroArray;
                if (byte[].class.equals(value.getClass())){
                    return toAvro(value, descriptor, avroSubSchema);
                } else {
                    Object[] array = (Object[]) value;
                    avroArray = new ArrayList(array.length);
                    for (Object item : array) {
                        avroArray.add(toAvro(item, descriptor, avroSubSchema));
                    }
                }
                return avroArray;
            case list:
                List list = (List) value;
                List avroList = new ArrayList(list.size());
                for (Object item : list) {
                    avroList.add(toAvro(item, descriptor, avroSubSchema));
                }
                return avroList;
            case map:
                Map<String, Object> map = (Map<String, Object>) value;
                Map<Utf8, Object> avroMap = new HashMap<>();
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    Object item = entry.getValue();
                    avroMap.put(new Utf8(entry.getKey()), toAvro(item, descriptor, avroSubSchema));
                }
                return avroMap;
        }

        return toAvro(value, descriptor, avroSubSchema);
    }

    public static Object toAvro(Object value, FieldDescriptor descriptor, Schema avroSubSchema) throws Exception {
        if (value == null)
            return null;

        if (avroSubSchema != null)
            return  new StreamableAvroRecord((Streamable) value, avroSubSchema);

        switch (descriptor.getValueType()){
            case jsonNode :
                return value instanceof JsonNode ? value.toString() : value;
            case date:
            case localDate:
                return DataTypes.int32Converter(value);
            case timestamp:
                return DataTypes.int64Converter(value);
            case classObject:
                return DataTypes.stringConverter(value);
            case binary:
                return ByteBuffer.wrap((byte[]) value);
        }

        return descriptor.getValueType().convert(value);
    }

}
