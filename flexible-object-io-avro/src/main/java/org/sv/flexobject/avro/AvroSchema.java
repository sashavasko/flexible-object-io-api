package org.sv.flexobject.avro;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.schema.FieldDescriptor;
import org.sv.flexobject.schema.SchemaElement;
import org.sv.flexobject.schema.SchemaException;
import org.sv.flexobject.schema.reflect.FieldWrapper;
import org.sv.flexobject.util.InstanceFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility methods for translating between the flexible-object internal schema
 * model and Apache Avro schemas and values.
 *
 * <p>The schema compiler maps {@link FieldDescriptor} metadata to Avro field
 * definitions, including nullable fields, arrays, lists, maps, nested
 * {@link Streamable} records, and Avro logical types for dates and timestamps.
 * The conversion helpers perform the matching runtime value transformations for
 * Avro input and output adapters.</p>
 */
public class AvroSchema {

    static {
        DataTypes.string.registerCustomConverter(Utf8.class, (v)->((Utf8)v).toString());
    }

    /**
     * Builds an Avro record schema for a registered {@link Streamable} class.
     *
     * @param dataClass streamable class with a registered flexible-object schema
     * @return Avro record schema representing the registered class schema
     * @throws RuntimeException when the internal schema cannot be resolved or compiled
     */
    public static Schema forClass(Class<? extends Streamable> dataClass) {
        try {
            return forSchema(org.sv.flexobject.schema.Schema.getRegisteredSchema(dataClass));
        } catch (Exception e) {
            throw new RuntimeException("Failed to compile Avro Schema for " + dataClass.getName(), e);
        }
    }

    /**
     * Builds an Avro record schema from an internal flexible-object schema.
     *
     * <p>Record fields are compiled with {@link #compileFields(org.sv.flexobject.schema.Schema)}.
     * Inner class names are normalized so the Avro record name does not contain
     * Java's {@code $} separator.</p>
     *
     * @param internalSchema source flexible-object schema
     * @return Avro record schema for the source schema
     * @throws NoSuchFieldException when field metadata cannot be resolved
     * @throws SchemaException when a field type cannot be mapped to Avro
     */
    public static Schema forSchema(org.sv.flexobject.schema.Schema internalSchema) throws NoSuchFieldException, SchemaException {
        List<Schema.Field> fields = compileFields(internalSchema);
        String name = internalSchema.getSimpleName();
        String namespace = internalSchema.getNamespace();
        if (name.contains("$")){
            String[] parts = name.split("\\$");
            for (int i = 0 ; i < parts.length - 1; ++i){
                namespace = namespace + "." + parts[i];
            }
            name = parts[parts.length-1];
        }
        return Schema.createRecord(name, null, namespace, false, fields);
    }

    /**
     * Compiles every field in an internal schema to Avro fields.
     *
     * <p>Non-binary fields are wrapped in a nullable union. Binary fields are
     * emitted as Avro {@code bytes} directly because the runtime conversion uses
     * {@link ByteBuffer} values.</p>
     *
     * @param internalSchema source flexible-object schema
     * @return fields suitable for an Avro record schema
     * @throws NoSuchFieldException when field metadata cannot be resolved
     * @throws SchemaException when a field type cannot be mapped to Avro
     */
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

    /**
     * Wraps a schema in a union that accepts {@code null} as the first branch.
     *
     * @param schema non-null Avro schema branch
     * @return {@code ["null", schema]}
     */
    public static Schema nullable(Schema schema){
        return Schema.createUnion(Schema.create(Schema.Type.NULL), schema);
    }

    /**
     * Compiles a single field descriptor to the Avro schema for that field's value.
     *
     * <p>Arrays and lists become Avro arrays, maps become Avro maps, and nested
     * {@link Streamable} fields are compiled as nested record schemas. Binary
     * array fields are treated as Avro {@code bytes} rather than arrays.</p>
     *
     * @param field field metadata to compile
     * @return Avro schema for the field value
     * @throws NoSuchFieldException when nested schema field metadata cannot be resolved
     * @throws SchemaException when the field type cannot be mapped to Avro
     */
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

    /**
     * Finds a record branch inside a nullable union schema.
     *
     * @param field Avro field whose schema may contain a record branch
     * @return the record schema branch, or {@code null} when none is present
     */
    public static Schema findRecordSchema(Schema.Field field) {
        return findContainerSchema(field, Schema.Type.RECORD);
    }

    /**
     * Finds an array branch inside a nullable union schema.
     *
     * @param field Avro field whose schema may contain an array branch
     * @return the array schema branch, or {@code null} when none is present
     */
    public static Schema findArraySchema(Schema.Field field) {
        return findContainerSchema(field, Schema.Type.ARRAY);
    }

    /**
     * Finds a map branch inside a nullable union schema.
     *
     * @param field Avro field whose schema may contain a map branch
     * @return the map schema branch, or {@code null} when none is present
     */
    public static Schema findMapSchema(Schema.Field field) {
        return findContainerSchema(field, Schema.Type.MAP);
    }

    /**
     * Finds a record branch inside a nullable union schema.
     *
     * @param schema Avro schema that may be a nullable record union
     * @return the record schema branch, or {@code null} when none is present
     */
    public static Schema findRecordSchema(Schema schema) {
        return findContainerSchema(schema, Schema.Type.RECORD);
    }

    /**
     * Finds an array branch inside a nullable union schema.
     *
     * @param schema Avro schema that may be a nullable array union
     * @return the array schema branch, or {@code null} when none is present
     */
    public static Schema findArraySchema(Schema schema) {
        return findContainerSchema(schema, Schema.Type.ARRAY);
    }

    /**
     * Finds a map branch inside a nullable union schema.
     *
     * @param schema Avro schema that may be a nullable map union
     * @return the map schema branch, or {@code null} when none is present
     */
    public static Schema findMapSchema(Schema schema) {
        return findContainerSchema(schema, Schema.Type.MAP);
    }

    /**
     * Finds a branch with the requested type inside a field's nullable union schema.
     *
     * @param field Avro field to inspect
     * @param containerType branch type to find
     * @return matching branch schema, or {@code null} when none is present
     */
    public static Schema findContainerSchema(Schema.Field field, Schema.Type containerType) {
        return findContainerSchema(field.schema(), containerType);
    }

    /**
     * Finds a branch with the requested type inside a nullable union schema.
     *
     * @param schema Avro schema to inspect
     * @param containerType branch type to find
     * @return matching branch schema, or {@code null} when {@code schema} is not
     * a union or does not contain the requested type
     */
    public static Schema findContainerSchema(Schema schema, Schema.Type containerType) {
        if (schema.isUnion()) {
            for (Schema item : schema.getTypes()) {
                if (item.getType() == containerType)
                    return item;
            }
        }
        return null;
    }

    /**
     * Converts a generic Avro record to a new {@link Streamable} instance.
     *
     * <p>The destination class is loaded from the Avro schema full name. When
     * the source is a {@link StreamableAvroRecord} wrapping the same class, the
     * wrapped object is returned directly.</p>
     *
     * @param src source generic Avro record
     * @param avroSchema schema describing {@code src}
     * @param <T> streamable destination type
     * @return populated streamable instance
     * @throws Exception when the destination class cannot be loaded or populated
     */
    public static <T extends Streamable> T convertGenericRecord(GenericRecord src, Schema avroSchema) throws Exception {
        Class<? extends Streamable> dstClass = (Class<? extends Streamable>) Class.forName(avroSchema.getFullName());

        if (src instanceof StreamableAvroRecord streamableAvroRecord){
            if (streamableAvroRecord.isWrapped(dstClass))
                return streamableAvroRecord.getWrapped();
        }

        Streamable dst = InstanceFactory.get(dstClass);
        return convertGenericRecord(src, avroSchema, dst);
    }

    /**
     * Converts a generic Avro record to a new instance of the supplied class.
     *
     * @param src source generic Avro record
     * @param dataClass destination streamable class
     * @param <T> streamable destination type
     * @return populated streamable instance
     * @throws Exception when the destination cannot be instantiated or populated
     */
    public static <T extends Streamable> T convertGenericRecord(GenericRecord src, Class<? extends Streamable> dataClass) throws Exception {
        if (src instanceof StreamableAvroRecord streamableAvroRecord){
            if (streamableAvroRecord.isWrapped(dataClass))
                return streamableAvroRecord.getWrapped();
        }

        Streamable dst = InstanceFactory.get(dataClass);
        return convertGenericRecord(src, forClass(dst.getClass()), dst);
    }

    /**
     * Copies values from a generic Avro record into an existing {@link Streamable}.
     *
     * <p>Nested records, arrays, lists, maps, UTF-8 strings, byte buffers, and
     * fields with registered {@link DataTypes} conversions are converted before
     * they are assigned to the destination.</p>
     *
     * @param src source generic Avro record
     * @param avroSchema schema describing {@code src}
     * @param dst destination object to populate
     * @param <T> streamable destination type
     * @return {@code dst}, cast to its concrete streamable type
     * @throws Exception when a field cannot be converted or assigned
     */
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

    /**
     * Converts an Avro map into a Java map with string keys and converted values.
     *
     * @param avroMap source map using Avro {@link Utf8} keys
     * @param recordSchema nested record schema for values, or {@code null} for scalar values
     * @param valueType flexible-object value type for scalar conversion
     * @return converted Java map
     * @throws Exception when a value cannot be converted
     */
    public static Map convertAvroMap(Map<Utf8, Object> avroMap, Schema recordSchema, DataTypes valueType) throws Exception {
        Map convertedMap = new HashMap<String, Object>();
        for (Map.Entry<Utf8,Object> entry : avroMap.entrySet()){
            convertedMap.put(convertAvroValue(entry.getKey(), null), convertAvroValue(entry.getValue(), recordSchema, valueType));
        }
        return convertedMap;
    }

    /**
     * Converts an Avro list into a Java list with converted item values.
     *
     * @param avroList source list
     * @param recordSchema nested record schema for items, or {@code null} for scalar values
     * @param valueType flexible-object value type for scalar conversion
     * @return converted Java list
     * @throws Exception when an item cannot be converted
     */
    public static List convertAvroList(List avroList, Schema recordSchema, DataTypes valueType) throws Exception {
        List convertedList = new ArrayList(avroList.size());
        for (Object item : avroList)
            convertedList.add(convertAvroValue(item, recordSchema, valueType));
        return convertedList;
    }

    /**
     * Converts an Avro generic array into a Java object array.
     *
     * @param avroArray source Avro array
     * @param recordSchema nested record schema for items, or {@code null} for scalar values
     * @param valueType flexible-object value type for scalar conversion
     * @return converted object array
     * @throws Exception when an item cannot be converted
     */
    public static Object[] convertAvroArray(GenericData.Array avroArray, Schema recordSchema, DataTypes valueType) throws Exception {
        Object[] convertedArray = new Object[avroArray.size()];
        for (int i = 0 ; i < avroArray.size() ; ++i){
            convertedArray[i] = convertAvroValue(avroArray.get(i), recordSchema, valueType);
        }
        return convertedArray;
    }

    /**
     * Converts one Avro value using an optional nested record schema.
     *
     * @param value Avro value to convert
     * @param recordSchema nested record schema, or {@code null} for scalar values
     * @return converted value
     * @throws Exception when the value cannot be converted
     */
    public static Object convertAvroValue(Object value, Schema recordSchema) throws Exception {
        return convertAvroValue(value, recordSchema, null);
    }

    /**
     * Converts one Avro value using optional nested record and scalar type metadata.
     *
     * @param value Avro value to convert
     * @param recordSchema nested record schema, or {@code null} for scalar values
     * @param valueType flexible-object value type for scalar conversion
     * @return converted value
     * @throws Exception when the value cannot be converted
     */
    public static Object convertAvroValue(Object value, Schema recordSchema, DataTypes valueType) throws Exception {
        if (value == null)
            return null;

        if (value instanceof GenericData.Record)
            return convertGenericRecord((GenericRecord) value, recordSchema);

        if (value instanceof Utf8)
            value = value.toString();

        return valueType == null ? value : valueType.convert(value);
    }

    /**
     * Converts a flexible-object field value to the representation expected by Avro.
     *
     * <p>Container fields are recursively converted. Map keys are converted to
     * {@link Utf8}, nested {@link Streamable} values are wrapped in
     * {@link StreamableAvroRecord}, and binary values are converted to
     * {@link ByteBuffer}.</p>
     *
     * @param descriptor field metadata describing the value
     * @param value source value from a streamable object
     * @return Avro-compatible value
     * @throws Exception when the value cannot be converted
     */
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

    /**
     * Converts a non-container value to the representation expected by Avro.
     *
     * @param value source field value
     * @param descriptor field metadata describing the value
     * @param avroSubSchema nested Avro schema for streamable values, or {@code null}
     * @return Avro-compatible value
     * @throws Exception when the value cannot be converted
     */
    public static Object toAvro(Object value, FieldDescriptor descriptor, Schema avroSubSchema) throws Exception {
        if (value == null)
            return null;

        if (avroSubSchema != null)
            return new StreamableAvroRecord((Streamable) value, avroSubSchema);

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
