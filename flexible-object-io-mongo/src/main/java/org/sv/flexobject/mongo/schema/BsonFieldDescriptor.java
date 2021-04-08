package org.sv.flexobject.mongo.schema;

import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.mongo.schema.annotations.BsonName;
import org.sv.flexobject.schema.AbstractFieldDescriptor;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.schema.Schema;
import org.sv.flexobject.schema.SchemaException;
import org.bson.BsonType;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

public class BsonFieldDescriptor extends AbstractFieldDescriptor {

    protected BsonType type;
    protected String bsonName;
    protected boolean isArray = false;
    protected boolean isDocument = false;

    public BsonFieldDescriptor(Class<?> dataClass, Field field, int order) {
        super(field.getName(), order);

        Class<?> fieldClass = field.getType();
        DataTypes dataType = DataTypes.valueOf(fieldClass);

        BsonName bn = field.getAnnotation(BsonName.class);
        bsonName = bn != null ? bn.name() : getName();
        org.sv.flexobject.mongo.schema.annotations.BsonType bt = field.getAnnotation(org.sv.flexobject.mongo.schema.annotations.BsonType.class);
        try {
            if (fieldClass.isArray() || List.class.isAssignableFrom(fieldClass)) {
                this.isArray = true;
                dataType = Schema.getRegisteredSchema(dataClass).getDescriptor(order).getValueType();
            } else if (StreamableWithSchema.class.isAssignableFrom(fieldClass) || Map.class.isAssignableFrom(fieldClass)) {
                this.isDocument = true;
                dataType = Schema.getRegisteredSchema(dataClass).getDescriptor(order).getValueType();
            }
        } catch (NoSuchFieldException | SchemaException e) {
        }

        type = bt != null ? bt.type() : genericBsonType(dataType);
    }

    private BsonType genericBsonType(DataTypes dataType) {
        switch(dataType){
            case bool: return BsonType.BOOLEAN;
            case classObject:
            case string:return BsonType.STRING;
            case binary:return BsonType.BINARY;
            case float64:return BsonType.DOUBLE;
            case timestamp:return BsonType.TIMESTAMP;
            case jsonNode:return BsonType.DOCUMENT;
            case int64:return BsonType.INT64;
            case int32:return BsonType.INT32;
            case localDate:
            case date:return BsonType.DATE_TIME;
        }
        return null;
    }

    public BsonType getType() {
        return type;
    }

    public boolean isArray() {
        return isArray;
    }

    public boolean isDocument() {
        return isDocument;
    }

    public String getBsonName() {
        return bsonName;
    }
}
