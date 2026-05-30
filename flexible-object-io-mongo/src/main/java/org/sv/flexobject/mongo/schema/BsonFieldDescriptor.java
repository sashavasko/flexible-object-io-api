package org.sv.flexobject.mongo.schema;

import org.bson.BsonType;
import org.bson.codecs.pojo.annotations.BsonProperty;
import org.bson.codecs.pojo.annotations.BsonRepresentation;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.mongo.schema.annotations.BsonName;
import org.sv.flexobject.schema.*;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BsonFieldDescriptor extends AbstractFieldDescriptor {

    protected DataTypes type;
    protected BsonType bsonType;
    protected String bsonName;
    protected boolean isArray = false;
    protected boolean isDocument = false;

    public BsonFieldDescriptor(Class<?> dataClass, Field field, int order) {
        super(field.getName(), order);

        Class<?> fieldClass = field.getType();
        DataTypes dataType = DataTypes.valueOf(fieldClass);
        this.type = dataType;

        BsonProperty bsonProperty = field.getAnnotation(BsonProperty.class);
        BsonName bn = field.getAnnotation(BsonName.class);
        bsonName = bn != null ? bn.name() : bsonProperty != null ? bsonProperty.value() : getName();

        try {
            if (fieldClass.isArray()
                    || List.class.isAssignableFrom(fieldClass)
                    || Set.class.isAssignableFrom(fieldClass)) {
                this.isArray = true;
                FieldDescriptor fieldDescriptor = Schema.getRegisteredSchema(dataClass).getDescriptor(order);
                dataType = fieldDescriptor.getValueType();
            } else if (Streamable.class.isAssignableFrom(fieldClass) || Map.class.isAssignableFrom(fieldClass)) {
                this.isDocument = true;
                dataType = Schema.getRegisteredSchema(dataClass).getDescriptor(order).getValueType();
            }
        } catch (NoSuchFieldException | SchemaException e) {
        }

        BsonRepresentation bsonRepresentation = field.getAnnotation(BsonRepresentation.class);
        org.sv.flexobject.mongo.schema.annotations.BsonType bt =  field.getAnnotation(org.sv.flexobject.mongo.schema.annotations.BsonType.class);
        bsonType = bt != null ? bt.type() : bsonRepresentation != null ? bsonRepresentation.value() : genericBsonType(dataType);
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

    public DataTypes getType() {
        return type;
    }

    public BsonType getBsonType() {
        return bsonType;
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
