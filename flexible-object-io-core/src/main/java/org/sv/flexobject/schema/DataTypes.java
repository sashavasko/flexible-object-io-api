package org.sv.flexobject.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.*;
import org.apache.commons.lang3.StringUtils;
import org.sv.flexobject.InAdapter;
import org.sv.flexobject.OutAdapter;
import org.sv.flexobject.StreamableWithSchema;
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
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    classObject(InAdapter::getClass, DataTypes::setString, DataTypes::classConverter),
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
        if (value instanceof Enum)
            return ((Enum)value).name();
        if (value instanceof Class)
            return ((Class)value).getName();
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
        if (value instanceof StreamableWithSchema)
            return ((StreamableWithSchema)value).toJson();
        if (value.getClass().isArray()){
            Object[] array = (Object[]) value;
            ArrayNode json = JsonNodeFactory.instance.arrayNode(array.length);
            for (Object o : array) {
                if (o instanceof String)
                    json.add((String)o);
                else
                    json.add(jsonConverter(o));
            }
            return json;
        }
        if (value instanceof List){
            List list = (List) value;
            ArrayNode json = JsonNodeFactory.instance.arrayNode(list.size());
            for (Object o : list) {
                if (o instanceof String)
                    json.add((String)o);
                else
                    json.add(jsonConverter(o));
            }
            return json;
        }
        if (value instanceof Map){
            Map map = (Map) value;
            ObjectNode json = JsonNodeFactory.instance.objectNode();
            for (Object o : map.entrySet()){
                Map.Entry entry = (Map.Entry) o;
                Object entryValue = entry.getValue();
                String key = stringConverter(entry.getKey());
                if (entryValue instanceof String)
                    json.set(key, JsonNodeFactory.instance.textNode((String)(entryValue)));
                else
                    json.set(key, jsonConverter(entryValue));
            }
            return json;
        }
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
        if (value instanceof String) {
            String dateString = (String) value;
            if (StringUtils.isNumeric(dateString)){
                int month;
                int day;
                int year;
                if (dateString.length() == 7){
                    month = dateString.charAt(0) - '0';
                    day =  Integer.valueOf(dateString.substring(1, 3));
                    year =  Integer.valueOf(dateString.substring(3));
                } else if (dateString.length() != 8){
                    throw new IllegalArgumentException("All numeric dates must be in either MMDDYYYY or YYYYMMDD format. Actual value :" + dateString);
                } else {
                    month = Integer.valueOf(dateString.substring(0, 2));
                    if (month > 12) {
                        year =  Integer.valueOf(dateString.substring(0, 4));
                        month = Integer.valueOf(dateString.substring(4, 6));
                        day =  Integer.valueOf(dateString.substring(6));
                    } else {
                        day =  Integer.valueOf(dateString.substring(2, 4));
                        year =  Integer.valueOf(dateString.substring(4));
                    }
                }
                return Date.valueOf(LocalDate.of(year, month, day));
            }
            return Date.valueOf(dateString);
        }
        if (value instanceof Timestamp)
            return new Date(((Timestamp)value).getTime());

        if (value instanceof Integer) {
            // Parquet julian date?
            return Date.valueOf(LocalDate.of(1970, 1, 1).plusDays((Integer) value));
        } else if (value instanceof Long)
            return new Date((Long) value);

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

    public static Class<?> classConverter(Object value) throws Exception {
        if (value == null || value instanceof Class)
            return (Class) value;
        String stringValue = stringConverter(value);
        if (stringValue != null){
            return Class.forName(stringValue);
        }

        throw new SchemaException("Attempting to convert value of type " + value.getClass().getName() + " to Class");
    }

    public static <T extends Enum<T>> T enumConverter(Object value, T defaultValue) throws Exception {
        if (value == null || value instanceof Enum)
            return (T) value;
        try {
            String stringValue = stringConverter(value);
            if (stringValue != null) {
                return Enum.valueOf(defaultValue.getDeclaringClass(), (String) value);
            }
        } catch (Exception e) {
        }
        return defaultValue;
    }

    public static Enum enumConverter(Object value, Class<? extends Enum> enumClass) throws Exception {
        if (value == null || value instanceof Enum)
            return (Enum) value;
        String stringValue = stringConverter(value);
        if (stringValue != null) {
            return Enum.valueOf(enumClass, (String) value);
        }

        throw new SchemaException("Attempting to convert value of type " + value.getClass().getName() + " to Enum " + enumClass.getName());
    }

    public static Set<Enum> enumSetConverter(Object value, Class<? extends Enum> enumClass, String emptyValue) throws Exception {
        if (value == null || value instanceof Set)
            return (Set) value;

        Set setOut = EnumSet.noneOf(enumClass);
        if (value instanceof ArrayNode) {
            for (JsonNode item : ((ArrayNode)value)) {
                String valueName = item.asText();
                if (!valueName.equalsIgnoreCase(emptyValue)) {
                    setOut.add(Enum.valueOf(enumClass, valueName));
                }
            }
        }else {
            String valueNames = stringConverter(value);
            if (StringUtils.isNotBlank(valueNames)) {
                if (valueNames.startsWith("["))
                    valueNames = valueNames.substring(1, valueNames.length() - 1)
                            ;
                for (String item : valueNames.split(",")) {
                    String valueName = item.trim();
                    if (valueName.startsWith("\""))
                        valueName = valueName.substring(1, valueName.length() - 1).trim();
                    if (!valueName.equalsIgnoreCase(emptyValue)) {
                        setOut.add(Enum.valueOf(enumClass, valueName));
                    }
                }
            }
        }
        return setOut;
    }

    public static String enumSetToString(Object value, Class<? extends Enum> enumClass, String emptyValue) throws Exception {
        Set bits = enumSetConverter(value, enumClass, emptyValue);
        String stringOut = emptyValue;
        if (!bits.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            for (Object bit : bits) {
                sb.append(((Enum)bit).name()).append(',');
            }
            sb.setLength(sb.length() - 1);
            stringOut = sb.toString();
        }

        return stringOut;
    }

    public static boolean isEmptyPrimitive(Object primitive){
        if (primitive == null)
            return true;

        Class<?> clazz = primitive.getClass();
        if (Integer.class.equals(clazz) || Short.class.equals(clazz) || Byte.class.equals(clazz)
                || Long.class.equals(clazz) || BigInteger.class.equals(clazz))
            return ((Number)primitive).longValue() == 0;
        if (Double.class.equals(clazz) || Float.class.equals(clazz))
            return ((Number)primitive).doubleValue() == 0;;
        if (Boolean.class.equals(clazz))
            return !((Boolean)primitive);
        if (Date.class.equals(clazz))
            return ((Date)primitive).getTime() == 0;
        if (LocalDate.class.equals(clazz))
            return ((LocalDate)primitive).getYear() == 0;
        if (Timestamp.class.equals(clazz))
            return ((Timestamp)primitive).getTime() == 0;
        return false;
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
            if (Class[].class.equals(clazz))
                return classObject;
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

        if (Class.class.equals(clazz))
            return classObject;

        if (JsonNode.class.isAssignableFrom(clazz))
            return jsonNode;

        if (StreamableWithSchema.class.isAssignableFrom(clazz))
            return jsonNode;

        // These are tricky since we need to know which Enum they belong to
        if (Enum.class.isAssignableFrom(clazz))
            return string;
        if (Set.class.isAssignableFrom(clazz))
            return string;

        return invalid;
    }

 }
