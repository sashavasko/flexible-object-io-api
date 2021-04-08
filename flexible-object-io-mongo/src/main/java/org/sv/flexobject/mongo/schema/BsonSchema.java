package org.sv.flexobject.mongo.schema;

import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.schema.*;
import org.sv.flexobject.schema.reflect.FieldWrapper;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.codec.binary.Hex;
import org.bson.*;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BsonSchema extends AbstractSchema {

    static {
        DataTypes.string.registerCustomConverter(ObjectId.class, (v)->((ObjectId)v).toHexString());
        DataTypes.string.registerCustomConverter(Binary.class, (v)-> Hex.encodeHexString(((Binary)v).getData()));
        DataTypes.binary.registerCustomConverter(ObjectId.class, (v)->((ObjectId)v).toByteArray());
        DataTypes.binary.registerCustomConverter(Binary.class, (v)-> ((Binary)v).getData());
        // TODO add nanos from ordinal
        DataTypes.timestamp.registerCustomConverter(BsonTimestamp.class, (v)-> {BsonTimestamp bson = (BsonTimestamp) v; return new Timestamp(bson.getTime()*1000L + bson.getInc()/1000000L);});
        DataTypes.timestamp.registerCustomConverter(BsonDateTime.class, (v)-> new Timestamp(((BsonDateTime)v).getValue()));

        DataTypes.date.registerCustomConverter(BsonTimestamp.class, (v)-> new Date(((BsonTimestamp)v).getTime()*1000L));
        DataTypes.date.registerCustomConverter(BsonDateTime.class, (v)-> new Date(((BsonDateTime)v).getValue()));
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

    public static BsonSchema getRegisteredSchema(Class<? extends StreamableWithSchema> dataClass) {
        String schemaName = dataClass.getName();
        return SchemaRegistry.getInstance().hasSchema(schemaName, BsonSchema.class) ?
            (BsonSchema) SchemaRegistry.getInstance().getSchema(dataClass.getName(), BsonSchema.class)
                : new BsonSchema(dataClass);
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

    public Document toBson(StreamableWithSchema value) throws Exception {
        Document bson = new Document();
        Schema schema = value.getSchema();
        for (int i = 0 ; i < fields.length ; ++i){
            FieldDescriptor schemaElement = (FieldDescriptor) schema.getFieldDescriptor(i);
            BsonFieldDescriptor bsonSchemaElement = (BsonFieldDescriptor) getFieldDescriptor(i);
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

        Class<? extends StreamableWithSchema> subSchema = descriptor.getSubschema();
        BsonSchema bsonSubSchema = subSchema == null ? null : (BsonSchema) SchemaRegistry.getInstance().getSchema(subSchema.getName(), BsonSchema.class);

        switch (structure) {
            case array:
                Object[] array = (Object[]) value;
                List bsonArray = new ArrayList(array.length);
                for (Object item : array) {
                    bsonArray.add(toBson(item, descriptor, bsonSchemaElement, bsonSubSchema));
                }
                return bsonArray;
            case list:
                List list = (List) value;
                List bsonList = new ArrayList(list.size());
                for (Object item : list) {
                    bsonList.add(toBson(item, descriptor, bsonSchemaElement, bsonSubSchema));
                }
                return bsonList;
            case map:
                Map<String, Object> map = (Map<String, Object>) value;
                Document bsonMap = new Document();
                for (Map.Entry<String, Object> entry : map.entrySet()) {
                    Object item = entry.getValue();
                    Object bsonValue = toBson(item, descriptor, bsonSchemaElement, bsonSubSchema);
                    if (bsonValue != null)
                        bsonMap.put(entry.getKey(), bsonValue);
                }
                return bsonMap;
        }

        return toBson(value, descriptor, bsonSchemaElement, bsonSubSchema);
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
                    return new BsonDateTime(DataTypes.dateConverter(value).getTime());
                case TIMESTAMP:
                    Timestamp timestamp = DataTypes.timestampConverter(value);
                    return new BsonTimestamp((int)(timestamp.getTime()/1000), timestamp.getNanos());
            }
        }

        return value;
    }

    public Object toBson(Object value, FieldDescriptor descriptor, BsonFieldDescriptor bsonSchemaElement, BsonSchema bsonSubSchema) throws Exception {
        if (value == null)
            return null;

        if (bsonSubSchema != null)
            return bsonSubSchema.toBson((StreamableWithSchema) value);

        switch (descriptor.getValueType()){
            case jsonNode :
                return toBson(value instanceof JsonNode ? value.toString() : value, bsonSchemaElement);
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

}
