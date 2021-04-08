package org.sv.flexobject.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.*;
import org.apache.commons.codec.binary.Hex;
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
import java.util.*;

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
    binary(InAdapter::getString, DataTypes::setBinary, DataTypes::binaryConverter),
    invalid((a,n)-> null, (a, n, o)->{}, o->o);

    protected BiFunctionWithException<InAdapter, String, Object, Exception> getter;
    protected TriConsumerWithException<OutAdapter, String, Object, Exception> setter;
    protected FunctionWithException converter;
    protected Map<Class,FunctionWithException> customConverters = new HashMap<>();

    DataTypes(BiFunctionWithException<InAdapter, String, Object, Exception> getter, TriConsumerWithException<OutAdapter, String, Object, Exception> setter, FunctionWithException converter) {
        this.getter = getter;
        this.setter = setter;
        this.converter = converter;
    }

    public void registerCustomConverter(Class sourceType, FunctionWithException converter){
        customConverters.put(sourceType, converter);
    }

    public void unregisterCustomConverter(Class sourceType){
        customConverters.remove(sourceType);
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

    protected Object applyCustomConverter(Object value, FunctionWithException defaultConverter) throws Exception {
        if (!customConverters.isEmpty()) {
            Class valueClass = value.getClass();
            FunctionWithException converter = customConverters.get(valueClass);
            while (converter == null && valueClass != null) {
                valueClass = valueClass.getSuperclass();
                if (valueClass != null)
                    converter = customConverters.get(valueClass);
            }
            if (converter != null)
                return converter.apply(value);
        }
        if (defaultConverter == null)
            throw new SchemaException("Attempting to convert value of type " + value.getClass().getName() + " to " + name());

        return defaultConverter.apply(value);
    }

    public static String stringConverter(Object value) throws Exception{
        if (value == null || value instanceof String)
            return (String) value;

        if (value instanceof byte[])
            return Hex.encodeHexString((byte[])value);
        if (value instanceof TextNode)
            return ((TextNode)value).asText();
        if (value instanceof JsonNode)
            return MapperFactory.getObjectWriter().writeValueAsString(value);
        if (value instanceof Enum)
            return ((Enum)value).name();
        if (value instanceof Class)
            return ((Class)value).getName();

        return (String) DataTypes.string.applyCustomConverter(value, (v)->v.toString());
    }

    public static void setString(OutAdapter adapter, String fieldName, Object value) throws Exception {
        adapter.setString(fieldName, stringConverter(value));
    }

    public static JsonNode jsonConverter(Object value) throws Exception {
        if (value == null || value instanceof JsonNode)
            return (JsonNode) value;
        if (value instanceof String) {
            String string = ((String) value).trim();
            if (string.startsWith("{") || string.startsWith("["))
                return MapperFactory.getObjectReader().readTree(string);
            else {
                String[] parts = string.split(",");
                ArrayNode jsonArray = JsonNodeFactory.instance.arrayNode(parts.length);
                for (String part : parts)
                    jsonArray.add(part);
                return jsonArray;
            }
        }
//        if (value instanceof byte[])
//            return JsonNodeFactory.instance.binaryNode((byte[])value);

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

        return (JsonNode) DataTypes.jsonNode.applyCustomConverter(value, (v)->MapperFactory.getObjectMapper().valueToTree(v));
    }

    public static void setJson(OutAdapter adapter, String fieldName, Object value) throws Exception {
        adapter.setJson(fieldName, jsonConverter(value));
    }

    public static Integer int32Converter(Object value) throws Exception {
        if (value == null || value instanceof Integer)
            return (Integer) value;
        if (value instanceof String)
            return Integer.valueOf(((String) value).trim());
        if (value instanceof ValueNode)
            return ((ValueNode)value).asInt();
        if (value instanceof Date) {
            long epochDay = ((Date) value).toLocalDate().toEpochDay();
            return (int)epochDay;
        }
        if (value instanceof LocalDate) {
            long epochDay = ((LocalDate) value).toEpochDay();
            return (int)epochDay;
        }

        return (Integer) DataTypes.int32.applyCustomConverter(value, (v)->((Number)v).intValue());
    }

    public static void setInt(OutAdapter adapter, String fieldName, Object value) throws Exception {
        adapter.setInt(fieldName, int32Converter(value));
    }

    public static Boolean boolConverter(Object value) throws Exception {
        if (value == null || value instanceof Boolean)
            return (Boolean) value;
        if (value instanceof String) {
            switch ((((String) value).trim()).toUpperCase()){
                case "TRUE" :
                case "YES" :
                case "Y" :
                case "1" :
                case "ON" : return true;
                default :
                    return false;
            }
        }
        if (value instanceof Number)
            return (((Number)value).intValue() > 0);
        if (value instanceof ValueNode)
            return ((ValueNode)value).asBoolean();

        return (Boolean) DataTypes.bool.applyCustomConverter(value, null);
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
        if (value instanceof Timestamp)
            return ((Timestamp)value).getTime();

        return (Long) DataTypes.int64.applyCustomConverter(value, (v)->((Number)v).longValue());
    }

    public static void setLong(OutAdapter adapter, String fieldName, Object value) throws Exception {
        adapter.setLong(fieldName, int64Converter(value));
    }

    public static Double float64Converter(Object value) throws Exception {
        if (value == null || value instanceof Double)
            return (Double) value;
        if (value instanceof String)
            return Double.valueOf(((String) value).trim());
        if (value instanceof ValueNode)
            return ((ValueNode)value).asDouble();

        return (Double) DataTypes.float64.applyCustomConverter(value, (v)->((Number)v).doubleValue());
    }

    public static void setDouble(OutAdapter adapter, String fieldName, Object value) throws Exception {
        adapter.setDouble(fieldName, float64Converter(value));
    }

    public static LocalDate localDateConverter(Object value) throws Exception {
        if (value == null || value instanceof LocalDate)
            return (LocalDate) value;
        return (LocalDate) DataTypes.localDate.applyCustomConverter(value, (v)->dateConverter(v).toLocalDate());
    }

    public static Date dateConverter(Object value) throws Exception {
        if (value == null || value instanceof Date)
            return (Date) value;
        if (value instanceof java.util.Date)
            return new Date(((java.util.Date)value).getTime());
        if (value instanceof LocalDate)
            return Date.valueOf((LocalDate) value);
        if (value instanceof ValueNode)
            return JsonInputAdapter.jsonNodeToDate((JsonNode)value);
        if (value instanceof String) {
            String dateString = ((String) value).trim();
            if (StringUtils.isNumeric(dateString)){
                int month;
                int day;
                int year;
                if (dateString.length() == 6){
                    // broken add_date made up of juris + sourceId
                    month = Integer.valueOf(dateString.substring(5));
                    year = Integer.valueOf(dateString.substring(0, 4));
                    day = 1;
                } else if (dateString.length() == 7){
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
            return Date.valueOf((String) value);
        }
        if (value instanceof Timestamp)
            return new Date(((Timestamp)value).getTime());

        if (value instanceof Integer) {
            // Days since Unix Epoch (see Parquet specification)
            return Date.valueOf(LocalDate.of(1970, 1, 1).plusDays((Integer) value));
        } else if (value instanceof Long)
            return new Date((Long) value);

        return (Date) DataTypes.date.applyCustomConverter(value, null);
    }

    public static void setDate(OutAdapter adapter, String fieldName, Object value) throws Exception {
        adapter.setDate(fieldName, dateConverter(value));
    }

    public static Timestamp timestampConverter(Object value) throws Exception {
        if (value == null || value instanceof Timestamp)
            return (Timestamp) value;
        if (value instanceof LocalDate)
            return new Timestamp(Date.valueOf((LocalDate) value).getTime());
        if (value instanceof String) {
            String timestampString = ((String) value).trim();
            if (StringUtils.isNumeric(timestampString) || timestampString.length() < "yyyy-mm-dd hh:mm:ss".length())
                return new Timestamp(dateConverter(value).getTime());
            return Timestamp.valueOf((String) value);
        }
        if (value instanceof ValueNode)
            return Timestamp.valueOf(((ValueNode)value).asText());
        if (value instanceof LocalDateTime)
            return Timestamp.valueOf((LocalDateTime)value);
        if (value instanceof Date)
            return new Timestamp(((Date)value).getTime());
        if (value instanceof java.util.Date)
            return new Timestamp(((java.util.Date)value).getTime());
        if (value instanceof Long)
            return new Timestamp((Long)value);
        if (value instanceof Integer)
            return new Timestamp(((Integer)value).longValue()*1000);

        return (Timestamp) DataTypes.timestamp.applyCustomConverter(value, null);
    }

    public static void setTimestamp(OutAdapter adapter, String fieldName, Object value) throws Exception {
        adapter.setTimestamp(fieldName, timestampConverter(value));
    }

    public static Class<?> classConverter(Object value) throws Exception {
        if (value == null || value instanceof Class)
            return (Class) value;
        String stringValue = stringConverter(value).trim();
        if (StringUtils.isNotEmpty(stringValue)){
            return Class.forName(stringValue);
        }

        return (Class<?>) DataTypes.classObject.applyCustomConverter(value, null);
    }

    public static <T extends Enum<T>> T enumConverter(Object value, T defaultValue) throws Exception {
        if (value == null || value instanceof Enum)
            return (T) value;
        try {
            String stringValue = stringConverter(value).trim();
            if (StringUtils.isNotEmpty(stringValue)){
                return Enum.valueOf(defaultValue.getDeclaringClass(), ((String) value).trim());
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
            return Enum.valueOf(enumClass, ((String) value).trim());
        }

        throw new SchemaException("Attempting to convert value of type " + value.getClass().getName() + " to Enum " + enumClass.getName());
    }

    public static Set<Enum> enumSetConverter(Object value, Class<? extends Enum> enumClass, String emptyValue) throws Exception {
        if (value == null || value instanceof Set)
            return (Set) value;

        Set setOut = EnumSet.noneOf(enumClass);
        if (value instanceof ArrayNode) {
            for (JsonNode item : ((ArrayNode)value)) {
                String valueName = item.asText().trim();
                if (!valueName.equalsIgnoreCase(emptyValue)) {
                    setOut.add(Enum.valueOf(enumClass, valueName));
                }
            }
        }else {
            String valueNames = stringConverter(value).trim();
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

    public static byte[] binaryConverter(Object value) throws Exception{
        if (value == null || value instanceof byte[])
            return (byte[]) value;

        if (value instanceof BinaryNode)
            return ((BinaryNode)value).binaryValue();
        if (value instanceof String)
            return Hex.decodeHex(((String)value).toCharArray());

        return (byte[]) DataTypes.binary.applyCustomConverter(value, (v)->v.toString());
    }

    public static void setBinary(OutAdapter adapter, String fieldName, Object value) throws Exception {
        adapter.setString(fieldName, value == null ? null : Hex.encodeHexString(binaryConverter(value)));
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
                    || int[].class.equals(clazz) || short[].class.equals(clazz))
                return int32;
            if (byte[].class.equals(clazz))
                return binary;
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
