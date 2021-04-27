package org.sv.flexobject.hadoop.streaming.parquet;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.*;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.schema.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import static org.apache.parquet.schema.OriginalType.LIST;
import static org.apache.parquet.schema.OriginalType.UTF8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.*;

public class ParquetSchema {

    public enum MapElementFields {
        key,
        value;

        public static MapElementFields forType(Type type){
            if (ELEMENT_OBJECT_NAME.equals(type.getName()))
                return value;
            return valueOf(type.getName());
        }
    }

    public static final String LIST_OBJECT_NAME = "list";
    public static final String ELEMENT_OBJECT_NAME = "element";
    public static final String KEY_OBJECT_NAME = MapElementFields.key.name();
    public static final String VALUE_OBJECT_NAME = MapElementFields.value.name();
    public static final String KEY_VALUE_OBJECT_NAME = "key_value";
    public static final String JSON_FIELD_NAME_LABEL = "fieldName";
    public static final String JSON_REPETITION_LABEL = "repetition";
    public static final String JSON_PRIMITIVE_LABEL = "primitive";
    public static final String JSON_ORIGINAL_TYPE_LABEL = "originalType";
    public static final String JSON_FIELDS_LABEL = "fields";
    public static final String JSON_ID_LABEL = "id";
    public static final String JSON_NAME_LABEL = "name";
    public static final String JSON_TYPE_LABEL = "type";

    public static Type makeScalarType(DataTypes type, String name) throws NoSuchFieldException, SchemaException {
        return makeScalarType(type, name, OPTIONAL);
    }

    public static Type makeScalarType(DataTypes type, String name, Type.Repetition repetition) throws NoSuchFieldException, SchemaException {
        switch (type) {
            case binary:
                return binaryField(name, repetition);
            case classObject:
            case string:
                return stringField(name, repetition);
            case jsonNode:
                return jsonField(name, repetition);
            case int32:
                return integerField(name, repetition);
            case int64:
                return longField(name, repetition);
            case float64:
                return doubleField(name, repetition);
            case bool:
                return booleanField(name, repetition);
            case date:
            case localDate:
                return dateField(name, repetition);
            case timestamp:
                return timestampField(name, repetition);
        }
        return null;
    }

    public static Type makeScalarType(Class<? extends StreamableWithSchema> subSchema, String name) throws NoSuchFieldException, SchemaException {
        List<Type> fields = parquetFieldsForClass(subSchema);
        return groupField(name, fields);
    }

    public static Type makeScalarType(FieldDescriptor descriptor, String name) throws NoSuchFieldException, SchemaException {
        Class<? extends StreamableWithSchema> subSchema = descriptor.getSubschema();
        if (subSchema != null){
            return makeScalarType(subSchema, name);
        }

        return makeScalarType(descriptor.getType(), descriptor.getName());
    }

    public static Type makeScalarType(FieldDescriptor descriptor) throws NoSuchFieldException, SchemaException {
        return makeScalarType(descriptor, descriptor.getName());
    }

    // The proper standard parquet way of having lists: requires 2 level redirection :
    // Optional Name(LIST){ Repeated list { Optional int32 element;}}
    // or : (but we do not care for it)
    // Optional Name(LIST){ Repeated list { Required int32 element;}}
    // or : (but we do not care for it)
    // Required Name(LIST){ Repeated list { Optional int32 element;}}
    // or : (but we do not care for it)
    // Required Name(LIST){ Repeated list { Required int32 element;}}
    // Abbreviated, but still compliant parquet way of having lists: requires 1 level redirection :
    // Optional Name(LIST){ Repeated int32 element; }   (in this case - element is required and there must be at least one,
    // or the whole list has to be null
    private static Type makeListType(FieldDescriptor fieldDescriptor) throws NoSuchFieldException, SchemaException {
        Class<? extends StreamableWithSchema> subSchema = fieldDescriptor.getSubschema();
        Type element;
        if (subSchema != null){
            element = makeScalarType(subSchema, ELEMENT_OBJECT_NAME);
        } else {
            element = makeScalarType(fieldDescriptor.getValueType(), ELEMENT_OBJECT_NAME);
        }

        if (element == null)
            return null;

        Type list = Types.buildGroup(REPEATED).addFields(element).named(LIST_OBJECT_NAME);
        return Types.buildGroup(OPTIONAL).as(OriginalType.LIST).addField(list).named(fieldDescriptor.getName());
    }

    public static Type makeMapType(FieldDescriptor fieldDescriptor) throws NoSuchFieldException, SchemaException {
        Class<? extends StreamableWithSchema> subSchema = fieldDescriptor.getSubschema();
        Type key = stringField(KEY_OBJECT_NAME, REQUIRED);
        Type value;
        if (subSchema != null){
            value = makeScalarType(subSchema, VALUE_OBJECT_NAME);
        } else {
            value = makeScalarType(fieldDescriptor.getValueType(), VALUE_OBJECT_NAME);
        }
        Type keyValue = Types.buildGroup(REPEATED).addFields(key, value).named(KEY_VALUE_OBJECT_NAME);
        return Types.buildGroup(OPTIONAL).as(OriginalType.MAP).addField(keyValue).named(fieldDescriptor.getName());
    }

    public static List<Type> parquetFieldsForClass(Class<? extends StreamableWithSchema> dataClass){
        Schema schema = Schema.getRegisteredSchema(dataClass);
        SchemaElement[] fields = schema.getFields();
        List<Type> parquetFields = new ArrayList<>(fields.length);
        try {
            for (SchemaElement schemaElement : fields) {
                FieldDescriptor descriptor = (FieldDescriptor) schemaElement.getDescriptor();
                Type parquetField = null;
                if (descriptor.isScalar()){
                    parquetField = makeScalarType(descriptor);
                } else {
                    switch (descriptor.getStructure()){
                        case array :
                        case list :
                            parquetField = makeListType(descriptor);
                            break;
                        case map :
                            parquetField = makeMapType(descriptor);
                            break;
                    }
                }
                if (parquetField != null)
                    parquetFields.add(parquetField);
            }
        }catch (Exception e){
            throw new RuntimeException("Failed to compile parquet schema for " + dataClass.getName(), e);
        }
        return parquetFields;
    }

    public static MessageType forClass(Class<? extends StreamableWithSchema> dataClass){
        return new MessageType(dataClass.getName(), parquetFieldsForClass(dataClass));
    }

    public static Class<? extends StreamableWithSchema> forType(GroupType type){
        try {
            return (Class<? extends StreamableWithSchema>) Thread.currentThread().getContextClassLoader().loadClass(type.getName());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to load data class " + type.getName(), e);
        }
    }

    public static MessageType forClass(String className){
        try {
            Class<?> schemaClass = Thread.currentThread().getContextClassLoader().loadClass(className);
            if (StreamableWithSchema.class.isAssignableFrom(schemaClass))
                return forClass((Class<? extends StreamableWithSchema>)schemaClass);
            else {
                try {
                    Method method = schemaClass.getMethod("getSchema");
                    return (MessageType)method.invoke(schemaClass.newInstance());
                } catch (NoSuchMethodException
                        | IllegalAccessException
                        | InstantiationException
                        | InvocationTargetException e) {
                    throw new RuntimeException("Requested class " + className +" must either implement StreamableWithSchema or define getSchema() method", e);
                }
            }
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to load data class " + className, e);
        }
    }

    public static List<Type> fromJson(ArrayNode json){
        List<Type> fields = new ArrayList<>(json.size());
        for (JsonNode field : json){
            fields.add(fromJsonField(field));
        }
        return fields;
    }

    private static Type fromJsonField(JsonNode json) {
        String name = json.get(JSON_FIELD_NAME_LABEL).asText();
        Type.Repetition repetition = Type.Repetition.OPTIONAL;
        if (json.has(JSON_REPETITION_LABEL))
            repetition = Type.Repetition.valueOf(json.get(JSON_REPETITION_LABEL).asText());

        boolean primitive = json.get(JSON_PRIMITIVE_LABEL).asBoolean();

        OriginalType originalType = null;
        if (json.has(JSON_ORIGINAL_TYPE_LABEL))
            originalType = OriginalType.valueOf(json.get(JSON_ORIGINAL_TYPE_LABEL).asText());

        if (primitive){
            PrimitiveType.PrimitiveTypeName typeName = PrimitiveType.PrimitiveTypeName.valueOf(json.get(JSON_TYPE_LABEL).asText());
            return new PrimitiveType(repetition, typeName, name, originalType);
        } else {
            List<Type> fields = fromJson((ArrayNode) json.get(JSON_FIELDS_LABEL));
            GroupType groupType = Types.buildGroup(repetition).as(originalType).named(name);
            groupType = groupType.withNewFields(fields);
            if (json.has(JSON_ID_LABEL)){
                groupType = groupType.withId(json.get(JSON_ID_LABEL).intValue());
            }
            return groupType;
        }
    }

    public static MessageType fromJson(ObjectNode json){
        String name = json.get(JSON_NAME_LABEL).asText();
        List<Type> fields = fromJson((ArrayNode) json.get(JSON_FIELDS_LABEL));

        return new MessageType(name, fields);
    }

    public static ArrayNode toJson(List<Type> fields) {
        ArrayNode fieldsNode = JsonNodeFactory.instance.arrayNode(fields.size());
        for (Type field : fields) {
            fieldsNode.add(toJson(field));
        }
        return fieldsNode;
    }

    public static ObjectNode toJson(Type field){
        ObjectNode json = JsonNodeFactory.instance.objectNode();
        json.put(JSON_FIELD_NAME_LABEL, field.getName());

        Type.Repetition repetition = field.getRepetition();
        if (repetition != Type.Repetition.OPTIONAL)
            json.put(JSON_REPETITION_LABEL, field.getRepetition().name());

        json.put(JSON_PRIMITIVE_LABEL, field.isPrimitive());

        OriginalType originalType = field.getOriginalType();
        if (originalType != null)
            json.put(JSON_ORIGINAL_TYPE_LABEL, originalType.name());

        if (field.isPrimitive()){
            json.put(JSON_TYPE_LABEL, field.asPrimitiveType().getPrimitiveTypeName().name());
        }else {
            json.set(JSON_FIELDS_LABEL, toJson(field.asGroupType().getFields()));
            if (field.asGroupType().getId() != null)
                json.put(JSON_ID_LABEL, field.asGroupType().getId().intValue());
        }
        return json;
    }

    public static ObjectNode toJson(MessageType parquet){
        ObjectNode json = JsonNodeFactory.instance.objectNode();
        json.put(JSON_NAME_LABEL, parquet.getName());
        json.set(JSON_FIELDS_LABEL, toJson(parquet.getFields()));
        return json;
    }

    public static MessageType fromAvro(org.apache.avro.Schema avro){
        return new AvroSchemaConverter().convert(avro);
    }

    public static MessageType fromAvro(org.apache.avro.Schema avro, Configuration conf){
        return new AvroSchemaConverter(conf).convert(avro);
    }

    public static org.apache.avro.Schema toAvro(MessageType parquet){
        return new AvroSchemaConverter().convert(parquet);
    }

    public static org.apache.avro.Schema toAvro(MessageType parquet, Configuration conf){
        return new AvroSchemaConverter(conf).convert(parquet);
    }

    //
    // We limit ourselves to the following data types for sanity sake:
    //
    public static Type binaryField(String name){
        return binaryField(name, OPTIONAL);
    }

    public static Type binaryField(String name, Type.Repetition repetition){
        return new PrimitiveType(repetition, BINARY, name);
    }

    public static Type stringField(String name){
        return stringField(name, OPTIONAL);
    }

    public static Type stringField(String name, Type.Repetition repetition){
        return new PrimitiveType(repetition, BINARY, name, UTF8);
    }

    public static Type jsonField(String name){
        return jsonField(name, OPTIONAL);
    }

    public static Type jsonField(String name, Type.Repetition repetition){
        return new PrimitiveType(repetition, BINARY, name, OriginalType.JSON);
    }

    public static Type integerField(String name){
        return integerField(name, OPTIONAL);
    }

    public static Type integerField(String name, Type.Repetition repetition){
        return new PrimitiveType(repetition, INT32, name);
    }

    public static Type longField(String name, Type.Repetition repetition){
        return new PrimitiveType(repetition, PrimitiveType.PrimitiveTypeName.INT64, name);
    }

    public static Type longField(String name){
        return longField(name, OPTIONAL);
    }

    public static Type doubleField(String name, Type.Repetition repetition){
        return new PrimitiveType(repetition, PrimitiveType.PrimitiveTypeName.DOUBLE, name);
    }

    public static Type doubleField(String name){
        return doubleField(name, OPTIONAL);
    }

    public static Type floatField(String name, Type.Repetition repetition){
        return new PrimitiveType(repetition, PrimitiveType.PrimitiveTypeName.FLOAT, name);
    }

    public static Type floatField(String name){
        return floatField(name, OPTIONAL);
    }

    public static Type booleanField(String name, Type.Repetition repetition){
        return new PrimitiveType(repetition, PrimitiveType.PrimitiveTypeName.BOOLEAN, name);
    }

    public static Type booleanField(String name){
        return booleanField(name, OPTIONAL);
    }

    public static Type dateField(String name, Type.Repetition repetition){
        return new PrimitiveType(repetition, INT32, name, OriginalType.DATE);
    }

    public static Type dateField(String name){
        return dateField(name, OPTIONAL);
    }

    public static Type timestampField(String name, Type.Repetition repetition){
        // TIMESTAMP_NICROS is depricated
        // This must match converters in DataTypes class
        return new PrimitiveType(repetition, PrimitiveType.PrimitiveTypeName.INT64, name, OriginalType.TIMESTAMP_MILLIS);
    }

    public static Type timestampField(String name){
        return timestampField(name, OPTIONAL);
    }

    public static Type groupField(String name, List<Type> fields){
        return new GroupType(Type.Repetition.OPTIONAL, name, fields);
    }
    public static List<Type> correctFieldsForSimpleLists(GroupType requestedSchema, GroupType fileSchema){
        List<Type> correctedFields = new ArrayList<>(requestedSchema.getFieldCount());
        for (Type requestedField : requestedSchema.getFields()){
            if (fileSchema.containsField(requestedField.getName())){
                if (requestedField.isPrimitive())
                    correctedFields.add(requestedField);
                else {
                    GroupType requestedGroupField = requestedField.asGroupType();
                    GroupType fileGroupField = fileSchema.getType(requestedField.getName()).asGroupType();

                    if (requestedField.getOriginalType() == LIST && !fileGroupField.containsField(LIST_OBJECT_NAME)){
                        correctedFields.add(fileGroupField);
                    } else {
                        correctedFields.add(correctGroupForSimpleLists(requestedGroupField, fileGroupField));
                    }
                }
            }
        }
        return correctedFields;
    }

    public static GroupType correctGroupForSimpleLists (GroupType requestedSchema, GroupType fileSchema){
        List<Type> correctedFields = correctFieldsForSimpleLists(requestedSchema, fileSchema);

        GroupType correctedGroup = Types.buildGroup(requestedSchema.getRepetition())
                .as(requestedSchema.getOriginalType())
                .named(requestedSchema.getName())
                .withNewFields(correctedFields);
        return requestedSchema.getId() != null ? correctedGroup.withId(requestedSchema.getId().intValue()) : correctedGroup;

    }

    public static MessageType correctSchemaForSimpleLists(MessageType requestedSchema, MessageType fileSchema) {
        List<Type> correctedFields = correctFieldsForSimpleLists(requestedSchema, fileSchema);
        return new MessageType(requestedSchema.getName(), correctedFields);
    }
}
