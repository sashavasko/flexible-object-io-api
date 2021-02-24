package org.sv.flexobject.hadoop.streaming.avro;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.schema.FieldDescriptor;
import org.sv.flexobject.schema.SchemaElement;
import org.sv.flexobject.schema.SchemaException;
import org.sv.flexobject.schema.reflect.FieldWrapper;

import java.util.ArrayList;
import java.util.List;

public class AvroSchema {

    public static Configuration overrideBackwardsOptions(Configuration conf){
        conf.setBoolean("parquet.strict.typing", false);
        conf.setBoolean("parquet.avro.add-list-element-records", false);
        conf.setBoolean("parquet.avro.write-old-list-structure", false);
        return conf;
    }

    public static Schema forClass(Class<? extends StreamableWithSchema> dataClass) {
        try {
            return forSchema(org.sv.flexobject.schema.Schema.getRegisteredSchema(dataClass));
        } catch (Exception e) {
            throw new RuntimeException("FGailed to compile Avro Schema for " + dataClass.getName(), e);
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
            FieldDescriptor descriptor = field.getDescriptor();
            fields.add(new Schema.Field(descriptor.getName(), nullable(compileField(descriptor))));
        }
        return fields;
    }

    public static Schema nullable(Schema schema){
        return Schema.createUnion(Schema.create(Schema.Type.NULL), schema);
    }

    public static Schema compileField(FieldDescriptor field) throws NoSuchFieldException, SchemaException {
        FieldWrapper.STRUCT structure = field.getStructure();
        Class<? extends StreamableWithSchema> subSchema = field.getSubschema();
        switch(structure){
            case array:
            case list:
                return Schema.createArray(nullable(compileField(field, subSchema)));
            case map:
                return Schema.createMap(nullable(compileField(field, subSchema)));
            default:
                return compileField(field, subSchema);
        }
    }

    private static Schema compileField(FieldDescriptor field, Class<? extends StreamableWithSchema> subSchema) throws NoSuchFieldException, SchemaException {
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
        }
        return null;
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
        for (Schema item : schema.getTypes()) {
            if (item.getType() == containerType)
                return item;
        }
        return null;
    }

}
