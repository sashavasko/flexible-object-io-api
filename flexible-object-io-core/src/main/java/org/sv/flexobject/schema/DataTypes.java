package org.sv.flexobject.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.databind.node.ValueNode;
import org.sv.flexobject.InAdapter;
import org.sv.flexobject.OutAdapter;
import org.sv.flexobject.json.JsonInputAdapter;
import org.sv.flexobject.json.MapperFactory;
import org.sv.flexobject.util.BiFunctionWithException;
import org.sv.flexobject.util.FunctionWithException;
import org.sv.flexobject.util.TriConsumerWithException;

import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;

public enum DataTypes {
    string(InAdapter::getString, DataTypes::setString, DataTypes::stringConverter),
    jsonNode(InAdapter::getJson, DataTypes::setJson, DataTypes::jsonConverter),
    int32(InAdapter::getInt, DataTypes::setInt, DataTypes::int32Converter),
    bool(InAdapter::getBoolean, DataTypes::setBoolean, DataTypes::boolConverter),
    int64(InAdapter::getLong,DataTypes::setLong, DataTypes::int64Converter),
    float64(InAdapter::getDouble, DataTypes::setDouble, DataTypes::float64Converter),
    date(InAdapter::getDate, DataTypes::setDate, DataTypes::dateConverter),
    timestamp(InAdapter::getTimestamp, DataTypes::setTimestamp, DataTypes::timestampConverter),
    localDate(InAdapter::getLocalDate, DataTypes::setDate, DataTypes::localDateConverter), //   LocalDate getLocalDate (String fieldName) throws Exception
    invalid((a,n)-> null, (a, n, o)->{}, o->o);

    protected BiFunctionWithException<InAdapter, String, Object, Exception> getter;
    protected TriConsumerWithException<OutAdapter, String, Object, Exception> setter;
    protected FunctionWithException converter;

    DataTypes(BiFunctionWithException<InAdapter, String, Object, Exception> getter, TriConsumerWithException<OutAdapter, String, Object, Exception> setter, FunctionWithException converter) {
        this.getter = getter;
        this.setter = setter;
        this.converter = converter;
    }

    public Object get(InAdapter adapter, String fieldName) throws Exception {
        return getter.apply(adapter, fieldName);
    }

    public Object get(InAdapter adapter, String fieldName, Object defaultValue) throws Exception {
        Object result = getter.apply(adapter, fieldName);
        return result == null ? defaultValue: result;
    }

    public void set(OutAdapter adapter, String fieldName, Object value) throws Exception {
        setter.accept(adapter, fieldName, value);
    }

    public Object convert(Object value) throws Exception {
        return converter.apply(value);
    }

    public static String stringConverter(Object value) throws Exception{
        if (value == null || value instanceof String)
            return (String) value;
        if (value instanceof ValueNode)
            return ((TextNode)value).asText();
        return value.toString();
    }

    public static void setString(OutAdapter adapter, String fieldName, Object value) throws Exception {
        adapter.setString(fieldName, stringConverter(value));
    }

    public static JsonNode jsonConverter(Object value) throws Exception {
        if (value == null || value instanceof JsonNode)
            return (JsonNode) value;
        if (value instanceof String)
            return MapperFactory.getObjectReader().readTree((String)value);
        return MapperFactory.getObjectMapper().valueToTree(value);
    }

    public static void setJson(OutAdapter adapter, String fieldName, Object value) throws Exception {
        adapter.setJson(fieldName, jsonConverter(value));
    }

    public static Integer int32Converter(Object value) throws Exception {
        if (value == null || value instanceof Integer)
            return (Integer) value;
        if (value instanceof String)
            return Integer.valueOf((String)value);
        if (value instanceof ValueNode)
            return ((ValueNode)value).asInt();
        return ((Number)value).intValue();
    }

    public static void setInt(OutAdapter adapter, String fieldName, Object value) throws Exception {
        adapter.setInt(fieldName, int32Converter(value));
    }

    public static Boolean boolConverter(Object value) throws Exception {
        if (value == null || value instanceof Boolean)
            return (Boolean) value;
        if (value instanceof String) {
            switch (((String)value).toUpperCase()){
                case "TRUE" :
                case "YES" :
                case "Y" :
                case "ON" : return true;
                default :
                    return false;
            }
        }
        if (value instanceof Number)
            return (((Number)value).intValue() > 0);
        if (value instanceof ValueNode)
            return ((ValueNode)value).asBoolean();

        throw new SchemaException("Attempting to convert value of type " + value.getClass().getName() + " to Boolean");
    }

    public static void setBoolean(OutAdapter adapter, String fieldName, Object value) throws Exception {
        adapter.setBoolean(fieldName, boolConverter(value));
    }

    public static Long int64Converter(Object value) throws Exception {
        if (value == null || value instanceof Long)
            return (Long) value;
        if (value instanceof String)
            return Long.valueOf((String)value);
        if (value instanceof ValueNode)
            return ((ValueNode)value).asLong();
        return ((Number)value).longValue();
    }

    public static void setLong(OutAdapter adapter, String fieldName, Object value) throws Exception {
        adapter.setLong(fieldName, int64Converter(value));
    }

    public static Double float64Converter(Object value) throws Exception {
        if (value == null || value instanceof Double)
            return (Double) value;
        if (value instanceof String)
            return Double.valueOf((String)value);
        if (value instanceof ValueNode)
            return ((ValueNode)value).asDouble();
        return ((Number)value).doubleValue();
    }

    public static void setDouble(OutAdapter adapter, String fieldName, Object value) throws Exception {
        adapter.setDouble(fieldName, float64Converter(value));
    }

    public static LocalDate localDateConverter(Object value) throws Exception {
        if (value == null || value instanceof LocalDate)
            return (LocalDate) value;
        return dateConverter(value).toLocalDate();
    }

    public static Date dateConverter(Object value) throws Exception {
        if (value == null || value instanceof Date)
            return (Date) value;
        if (value instanceof LocalDate)
            return Date.valueOf((LocalDate) value);
        if (value instanceof ValueNode)
            return JsonInputAdapter.jsonNodeToDate((JsonNode)value);
        if (value instanceof String)
            return Date.valueOf((String)value);
        if (value instanceof Timestamp)
            return new Date(((Timestamp)value).getTime());

        throw new SchemaException("Attempting to convert value of type " + value.getClass().getName() + " to Date");
    }

    public static void setDate(OutAdapter adapter, String fieldName, Object value) throws Exception {
        adapter.setDate(fieldName, dateConverter(value));
    }

    public static Timestamp timestampConverter(Object value) throws Exception {
        if (value == null || value instanceof Timestamp)
            return (Timestamp) value;
        if (value instanceof LocalDate)
            return new Timestamp(Date.valueOf((LocalDate) value).getTime());
        if (value instanceof String)
            return Timestamp.valueOf((String)value);
        if (value instanceof ValueNode)
            return Timestamp.valueOf(((ValueNode)value).asText());
        if (value instanceof LocalDateTime)
            return Timestamp.valueOf((LocalDateTime)value);
        if (value instanceof Date)
            return new Timestamp(((Date)value).getTime());

        throw new SchemaException("Attempting to convert value of type " + value.getClass().getName() + " to Timestamp");
    }

    public static void setTimestamp(OutAdapter adapter, String fieldName, Object value) throws Exception {
        adapter.setTimestamp(fieldName, timestampConverter(value));
    }

    public static DataTypes valueOf(Class<?> clazz){
        if (clazz.getName().startsWith("[")){
            if (String[].class.equals(clazz))
                return string;
            if (Integer[].class.equals(clazz) || Short[].class.equals(clazz) || Byte[].class.equals(clazz)
                || int[].class.equals(clazz) || short[].class.equals(clazz) || byte[].class.equals(clazz))
                return int32;
            if (Long[].class.equals(clazz) || BigInteger[].class.equals(clazz)
                || long[].class.equals(clazz))
                return int64;
            if (Double[].class.equals(clazz) || Float[].class.equals(clazz)
                || double[].class.equals(clazz) || float[].class.equals(clazz))
                return float64;
            if (Boolean[].class.equals(clazz)
                || boolean[].class.equals(clazz))
                return bool;
            if (Date[].class.equals(clazz))
                return date;
            if (LocalDate[].class.equals(clazz))
                return localDate;
            if (Timestamp[].class.equals(clazz))
                return timestamp;
            return invalid;
        }
        if (String.class.equals(clazz))
            return string;
        if (Integer.class.equals(clazz) || Short.class.equals(clazz) || Byte.class.equals(clazz)
            || int.class.equals(clazz) || short.class.equals(clazz) || byte.class.equals(clazz))
            return int32;
        if (Long.class.equals(clazz) || BigInteger.class.equals(clazz)
                || long.class.equals(clazz))
            return int64;
        if (Double.class.equals(clazz) || Float.class.equals(clazz)
            || double.class.equals(clazz) || float.class.equals(clazz))
            return float64;
        if (Boolean.class.equals(clazz)
                || boolean.class.equals(clazz))
            return bool;
        if (Date.class.equals(clazz))
            return date;
        if (LocalDate.class.equals(clazz))
            return localDate;
        if (Timestamp.class.equals(clazz))
            return timestamp;
        if (JsonNode.class.isAssignableFrom(clazz))
            return jsonNode;
        return invalid;
    }

 }
