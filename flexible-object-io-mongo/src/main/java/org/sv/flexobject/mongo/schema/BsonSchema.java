package org.sv.flexobject.mongo.schema;

import org.apache.commons.codec.binary.Hex;
import org.bson.*;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.mongo.json.BsonToJsonConverter;
import org.sv.flexobject.schema.*;
import org.sv.flexobject.schema.reflect.FieldWrapper;
import org.sv.flexobject.util.FunctionWithException;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.*;

public class BsonSchema extends AbstractSchema {
    public static FunctionWithException bsonTimestampConverter = (v)-> {BsonTimestamp bson = (BsonTimestamp) v; return new Timestamp(bson.getTime()*1000L + bson.getInc()/1000000L);};
    public static FunctionWithException bsonDateConverter = (v)-> new Date(((BsonDateTime)v).getValue());

    static {
        DataTypes.string.registerCustomConverter(ObjectId.class, (v)->((ObjectId)v).toHexString());
        DataTypes.string.registerCustomConverter(BsonString.class, (v)->((BsonString)v).getValue());
        DataTypes.string.registerCustomConverter(BsonObjectId.class, (v)->((BsonObjectId)v).getValue().toHexString());
        DataTypes.string.registerCustomConverter(Binary.class, (v)-> Hex.encodeHexString(((Binary)v).getData()));
        DataTypes.string.registerCustomConverter(BsonBinary.class, (v)-> Hex.encodeHexString(((BsonBinary)v).getData()));
        DataTypes.binary.registerCustomConverter(ObjectId.class, (v)->((ObjectId)v).toByteArray());
        DataTypes.binary.registerCustomConverter(BsonObjectId.class, (v)->((BsonObjectId)v).getValue().toByteArray());
        DataTypes.binary.registerCustomConverter(Binary.class, (v)-> ((Binary)v).getData());
        DataTypes.binary.registerCustomConverter(BsonBinary.class, (v)-> ((BsonBinary)v).getData());
        // TODO add nanos from ordinal
        DataTypes.timestamp.registerCustomConverter(BsonTimestamp.class, bsonTimestampConverter);
        DataTypes.timestamp.registerCustomConverter(BsonDateTime.class, (v)-> new Timestamp(((BsonDateTime)v).getValue()));

        DataTypes.date.registerCustomConverter(BsonTimestamp.class, (v)-> new Date(((BsonTimestamp)v).getTime()*1000L));
        DataTypes.date.registerCustomConverter(BsonDateTime.class, bsonDateConverter);
        DataTypes.localDate.registerCustomConverter(BsonTimestamp.class, (v)-> new Date(((BsonTimestamp)v).getTime()*1000L).toLocalDate());
        DataTypes.localDate.registerCustomConverter(BsonDateTime.class, (v)-> new Date(((BsonDateTime)v).getValue()).toLocalDate());

        DataTypes.int64.registerCustomConverter(BsonNumber.class, (v)->((BsonNumber)v).longValue());
        DataTypes.int32.registerCustomConverter(BsonNumber.class, (v)->((BsonNumber)v).intValue());
        DataTypes.float64.registerCustomConverter(BsonNumber.class, (v)->((BsonNumber)v).doubleValue());
        DataTypes.bool.registerCustomConverter(BsonBoolean.class, (v)->((BsonBoolean)v).getValue());

    }

    public BsonSchema(Class<?> dataClass) {
        super(dataClass);
        List<SchemaElement> fieldList = new ArrayList<>();

        addClassFields(dataClass.getSuperclass(), fieldList);
        addClassFields(dataClass, fieldList);

        setFields(fieldList);

        SchemaRegistry.getInstance().registerSchema(this);
    }

    public static BsonSchema getRegisteredSchema(Class<?> dataClass) {
        String schemaName = dataClass.getName();
        return SchemaRegistry.getInstance().hasSchema(schemaName, BsonSchema.class) ?
                SchemaRegistry.getInstance().getSchema(dataClass.getName(), BsonSchema.class)
                : new BsonSchema(dataClass);
    }

    public BsonFieldDescriptor getFieldDescriptor(int i) {
        return (BsonFieldDescriptor) super.getFieldDescriptor(i);
    }

    public BsonFieldDescriptor getFieldDescriptor(String fieldName) {
        return (BsonFieldDescriptor) super.getFieldDescriptor(fieldName);
    }

    private void addClassFields(Class<?> dataClass, List<SchemaElement> fieldList) {
        if (dataClass != null && !StreamableWithSchema.class.equals(dataClass)) {
            Field[] fields = dataClass.getDeclaredFields();

            for (Field field : fields) {
                if (Schema.isStreamableField(field)) {
                    BsonFieldDescriptor descriptor = new BsonFieldDescriptor(dataClass, field, fieldList.size());
                    fieldList.add(new SimpleSchemaElement(descriptor));
                }
            }
        }
    }

    private String getBsonFieldName(String fieldName) {
        return getFieldDescriptor(fieldName).getBsonName();
    }

    public static Document serialize(Streamable value) throws Exception {
        return getRegisteredSchema(value.getClass()).toBson(value);
    }

    public Document toBson(Streamable value) throws Exception {
        Document bson = new Document();
        Schema schema = value.getSchema();
        for (int i = 0 ; i < fields.length ; ++i){
            FieldDescriptor schemaElement = schema.getDescriptor(i);
            BsonFieldDescriptor bsonSchemaElement = getFieldDescriptor(i);
            Object bsonValue = toBson(schemaElement, bsonSchemaElement, value.get(schemaElement));
            if (bsonValue != null)
                bson.put(bsonSchemaElement.getBsonName(), bsonValue);
        }
        return bson;
    }

    public Object toBson(FieldDescriptor descriptor, BsonFieldDescriptor bsonSchemaElement, Object value) throws Exception {
        if (value == null)
            return null;

        FieldWrapper.STRUCT structure = descriptor.getStructure();

        Class<? extends Streamable> subSchema = descriptor.getSubschema();
        BsonSchema bsonSubSchema = subSchema == null ? null : getRegisteredSchema(subSchema);

        switch (structure) {
            case array:
                if (descriptor.getType() == DataTypes.binary){
                    return new BsonBinary((byte[])value);
                } else {
                    Object[] array = (Object[]) value;
                    List bsonArray = new ArrayList(array.length);
                    for (Object item : array) {
                        bsonArray.add(toBson(item, descriptor, bsonSchemaElement, bsonSubSchema));
                    }
                    return bsonArray;
                }

            case list:
                Collection list = (Collection) value;
                List bsonList = new ArrayList(list.size());
                for (Object item : list) {
                    bsonList.add(toBson(item, descriptor, bsonSchemaElement, bsonSubSchema));
                }
                return bsonList;
            case map:
                Map<Object,Object> map = (Map) value;
                Document bsonMap = new Document();
                for (Map.Entry<Object,Object> entry : map.entrySet()) {
                    Object item = entry.getValue();
                    Object bsonValue = toBson(item, descriptor, bsonSchemaElement, bsonSubSchema);
                    if (bsonValue != null)
                        bsonMap.put(DataTypes.stringConverter(entry.getKey()), bsonValue);
                }
                return bsonMap;
        }

        return toBson(value, descriptor, bsonSchemaElement, bsonSubSchema);
    }

    public static BsonTimestamp toBsonTimestamp(Object value) throws Exception {
        return toBsonTimestamp(DataTypes.timestampConverter(value));
    }

    public static BsonTimestamp toBsonTimestamp(Timestamp timestamp) {
        return new BsonTimestamp((int)(timestamp.getTime()/1000), timestamp.getNanos());
    }

    public static BsonDateTime toBsonDateTime(Date date) {
        return new BsonDateTime(date.getTime());
    }

    public Object toBson(Object value, BsonFieldDescriptor bsonSchemaElement) throws Exception {
        if (value == null)
            return null;

        if (bsonSchemaElement != null){
            switch (bsonSchemaElement.getType()){
                case INT32: return new BsonInt32(DataTypes.int32Converter (value));
                case BINARY:
                    if (value instanceof String){
                        return new Binary(Hex.decodeHex(((String) value).toCharArray()));
                    }
                    return new Binary(DataTypes.stringConverter(value).getBytes(StandardCharsets.UTF_8));
                case DOUBLE: return new BsonDouble(DataTypes.float64Converter(value));
                case STRING: return new BsonString(DataTypes.stringConverter(value));
                case INT64: return new BsonInt64(DataTypes.int64Converter (value));
                case BOOLEAN: return new BsonBoolean(DataTypes.boolConverter(value));
                case OBJECT_ID: return new ObjectId(DataTypes.stringConverter(value));
                case DATE_TIME:
                    return toBsonDateTime(DataTypes.dateConverter(value));
                case TIMESTAMP:
                    return toBsonTimestamp(value);
                case DOCUMENT: return Document.parse(value.toString());
            }
        }

        return value;
    }

    public Object toBson(Object value, FieldDescriptor descriptor, BsonFieldDescriptor bsonSchemaElement, BsonSchema bsonSubSchema) throws Exception {
        if (value == null)
            return null;

        if (bsonSubSchema != null)
            return bsonSubSchema.toBson((Streamable) value);

        switch (descriptor.getValueType()){
            case jsonNode :
                return toBson(value, bsonSchemaElement);
            case date:
                return toBson(DataTypes.dateConverter(value), bsonSchemaElement);
            case localDate:
                return toBson(DataTypes.localDateConverter(value), bsonSchemaElement);
            case timestamp:
                return toBson(DataTypes.timestampConverter(value), bsonSchemaElement);
            case classObject:
                return toBson(DataTypes.stringConverter(value), bsonSchemaElement);
        }

        return toBson(descriptor.getValueType().convert(value), bsonSchemaElement);
    }

    public <T extends Streamable> T fromBson(Map<String, ?> src) throws Exception {
        Class<? extends Streamable> dstClass = (Class<? extends Streamable>) Class.forName(getName());
        return (T) dstClass.cast(fromBson(src, dstClass, this));
    }

    public static <T extends Streamable> T fromBson(Map<String, ?> src, Class<? extends Streamable> dstClass) throws Exception {
        BsonSchema bsonSchema = BsonSchema.getRegisteredSchema(dstClass);
        return (T) dstClass.cast(fromBson(src, dstClass, bsonSchema));
    }
    public static Streamable fromBson(Map<String, ?> src, Class<? extends Streamable> dstClass, BsonSchema bsonSchema) throws Exception {
        Streamable dst = dstClass.newInstance();
        for (SchemaElement field : dst.getSchema().getFields()) {
            String fieldName = field.getDescriptor().getName();
            FieldDescriptor descriptor = (FieldDescriptor) field.getDescriptor();
            Class<? extends Streamable> recordSchema = descriptor.getSubschema();
            Object value = src.get(bsonSchema.getBsonFieldName(fieldName));
            DataTypes valueType = descriptor.getValueType();
            if (value == null){
                dst.set(fieldName, null);
            } else {
                if (value instanceof List) {
                    dst.set(fieldName, fromBsonList((List) value, recordSchema, valueType));
                } else if (recordSchema != null) {
                    dst.set(fieldName, fromBson((Map<String, ?>) value, recordSchema));
                } else
                    dst.set(fieldName, fromBsonValue(value, null, value instanceof Map ? valueType : null));
            }
        }
        return dst;
    }

    public static List fromBsonList(List bsonList, Class<? extends Streamable> recordSchema, DataTypes valueType) throws Exception {
        List convertedList = new ArrayList(bsonList.size());
        for (Object item : bsonList)
            convertedList.add(fromBsonValue(item, recordSchema, valueType));
        return convertedList;
    }

    public static Object fromBsonValue(Object value, Class<? extends Streamable>  recordSchema) throws Exception {
        return fromBsonValue(value, recordSchema, null);
    }

    public static Object fromBsonValue(Object value, Class<? extends Streamable>  recordSchema, DataTypes valueType) throws Exception {
        if (value == null || value instanceof BsonNull)
            return null;

        BsonToJsonConverter bsonConverter = new BsonToJsonConverter();

        if (value instanceof Map) {
            Map<String, ?> map = (Map<String, ?>) value;
            if (recordSchema != null)
                return fromBson(map, recordSchema);
            else {// actual map of values
                if (valueType == DataTypes.jsonNode){
                    return bsonConverter.convert(value);
                }else {
                    Map<String, Object> convertedMap = new HashMap<>();
                    for (Map.Entry<String, ?> entry : map.entrySet()) {
                        convertedMap.put(entry.getKey(), fromBsonValue(entry.getValue(), recordSchema, valueType));
                    }
                    return convertedMap;
                }
            }
        }

        return valueType == null ? value : valueType.convert(value);
    }
}
