package org.sv.flexobject.arrow;

import org.sv.flexobject.Streamable;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.schema.FieldDescriptor;
import org.sv.flexobject.schema.SchemaElement;
import org.sv.flexobject.schema.SchemaException;
import org.sv.flexobject.schema.reflect.FieldWrapper;
import com.google.protobuf.ByteString;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ArrowSchema {

    public static Schema forClass(Class <? extends Streamable> streamableClass) {
        try {
            return forSchema(org.sv.flexobject.schema.Schema.getRegisteredSchema(streamableClass));
        } catch (NoSuchFieldException e) {
            throw new SchemaException("Failed to compile Arrow schema for class " + streamableClass, e);
        }
    }

    public static Schema forSchema(org.sv.flexobject.schema.Schema internalSchema) throws NoSuchFieldException, SchemaException {
        List<Field> fields = compileFields(internalSchema);
        Map<String, String> metadata = new HashMap<>();
        metadata.put("name", internalSchema.getName());
        return new Schema(fields, metadata);
    }

    private static List<Field> compileFields(org.sv.flexobject.schema.Schema internalSchema) throws NoSuchFieldException {
        SchemaElement[] internalFields = internalSchema.getFields();
        List<Field> fields = new ArrayList<>(internalFields.length);
        for (SchemaElement field : internalFields){
            FieldDescriptor descriptor = (FieldDescriptor) field.getDescriptor();
            if (descriptor.getValueType() == DataTypes.binary){
                fields.add(new Field(descriptor.getName(), FieldType.nullable(new ArrowType.Binary()), null));
            } else {
                FieldWrapper.STRUCT structure = descriptor.getStructure();
                FieldType fieldType = null;
                switch(structure) {
                    case list:
                    case array:
                        fieldType = new FieldType(true, new ArrowType.List(), null);
                        break;

                    case map:
                        fieldType = new FieldType(true, new ArrowType.Map(false), null);
                        break;

                    default:
                        fieldType = new FieldType(true, compileFieldType(descriptor), null);
                }
                List<Field> children = compileChildren(descriptor);
                fields.add(new Field(descriptor.getName(), fieldType, children));
            }
        }
        return fields;
    }

    private static ArrowType compileFieldType(FieldDescriptor descriptor) throws NoSuchFieldException {
        if (descriptor.getSubschema() != null)
            return new ArrowType.Struct();
        return compileFieldType(descriptor.getType());
    }

    private static ArrowType compileFieldType(DataTypes dataType) throws NoSuchFieldException {
        switch (dataType) {
            case binary:
                return new ArrowType.Binary();
            case jsonNode:
            case string:
            case classObject:
                return new ArrowType.Utf8();
            case bool:
                return new ArrowType.Bool();
            case int32:
                return new ArrowType.Int(32, true);
            case int64:
                return new ArrowType.Int(64, true);
            case float64:
                return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case date:
            case localDate:
                return new ArrowType.Date(DateUnit.DAY);
            case timestamp:
                return new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC");
        }
        return null;
    }

    private static List<Field> compileChildren(FieldDescriptor descriptor) throws NoSuchFieldException {
        FieldWrapper.STRUCT structure = descriptor.getStructure();
        Class<? extends Streamable> subSchema = descriptor.getSubschema();
        if ((structure == FieldWrapper.STRUCT.scalar && subSchema == null)
            || descriptor.getValueType() == DataTypes.binary)
            return null;

        List<Field> children = new ArrayList<>();
        List<Field> subFields = null;

        if (subSchema != null) {
            subFields = compileFields(org.sv.flexobject.schema.Schema.getRegisteredSchema(subSchema));
            if (structure == FieldWrapper.STRUCT.scalar){
                children.addAll(subFields);
            } else {
                if (structure == FieldWrapper.STRUCT.map){
                    List<Field> mapEntry = new ArrayList<>();
                    mapEntry.add(new Field("key", FieldType.notNullable(compileFieldType(descriptor.getKeyType())), null));
                    mapEntry.add(new Field("value", FieldType.nullable(new ArrowType.Struct()), subFields));
                    Field container = new Field("element", FieldType.notNullable(new ArrowType.Struct()), mapEntry);
                    children.add(container);
                } else {
                    children.add(new Field("element", FieldType.notNullable(new ArrowType.Struct()), subFields));
                }
            }
        } else {
            if (structure == FieldWrapper.STRUCT.map){
                List<Field> mapEntry = new ArrayList<>();
                mapEntry.add(new Field("key", FieldType.notNullable(compileFieldType(descriptor.getKeyType())), null));
                mapEntry.add(new Field("value", FieldType.nullable(compileFieldType(descriptor.getValueType())), null));
                Field container = new Field("element", FieldType.notNullable(new ArrowType.Struct()), mapEntry);
                children.add(container);
            } else {
                children.add(new Field("element", FieldType.notNullable(compileFieldType(descriptor.getValueType())), null));
            }
        }

        return children;
    }

    public static boolean isList(Field field){
        ArrowType.ArrowTypeID typeId = field.getFieldType().getType().getTypeID();
        return typeId == ArrowType.ArrowTypeID.List
                || typeId == ArrowType.ArrowTypeID.LargeList
                || typeId == ArrowType.ArrowTypeID.ListView
                || typeId == ArrowType.ArrowTypeID.FixedSizeList;
    }

    public static boolean isMap(Field field){
        return field.getFieldType().getType().getTypeID() == ArrowType.ArrowTypeID.Map;
    }

    public static boolean isStruct(Field field){
        return field.getFieldType().getType().getTypeID() == ArrowType.ArrowTypeID.Struct;
    }

    public static boolean isBinary(Field field){
        return field.getFieldType().getType().getTypeID() == ArrowType.ArrowTypeID.Binary;
    }

    public static Field findField(Field parent, String fieldName) {
        return findField(parent.getChildren(), fieldName);
    }
    public static Field findField(List<Field> fields, String fieldName) {
        for (Field field : fields) {
            if (fieldName.equals(field.getName())) {
                return field;
            }
        }
        return null;
    }

    public static boolean isSimple(Field field) {
        return field.getChildren() == null || field.getChildren().size() == 0;
    }

    public static List<Field> getMapValueArrowSchema(List<Field> children) {
        if (children.size() == 1){
            Field entryStruct = children.get(0);
            Field valueField = findField(entryStruct.getChildren(), "value");
            return valueField.getChildren();
        }
        return null;
    }

    public static ByteString asByteString(Class <? extends Streamable> schemaClass) {
        return toByteString(forClass(schemaClass));
    }

    public static ByteString toByteString(Schema schema) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            MessageSerializer.serialize(
                    new WriteChannel(Channels.newChannel(baos)), schema, IpcOption.DEFAULT);
            return ByteString.copyFrom(baos.toByteArray());
        } catch (IOException e) {
            throw new SchemaException("Failed to serialize Schema", e);
        }
    }


}
