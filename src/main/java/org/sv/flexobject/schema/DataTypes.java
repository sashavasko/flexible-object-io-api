package org.sv.flexobject.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.databind.node.ValueNode;
import org.sv.flexobject.InAdapter;
import org.sv.flexobject.OutAdapter;
import org.sv.flexobject.SaveException;
import org.sv.flexobject.json.JsonInputAdapter;
import org.sv.flexobject.json.MapperFactory;
import org.sv.flexobject.util.BiFunctionWithException;
import org.sv.flexobject.util.TriConsumerWithException;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;

public enum DataTypes {
    string(InAdapter::getString, DataTypes::setString),
    jsonNode(InAdapter::getJson, DataTypes::setJson),
    int32(InAdapter::getInt, DataTypes::setInt),
    bool(InAdapter::getBoolean, DataTypes::setBoolean),
    int64(InAdapter::getLong,DataTypes::setLong),
    float64(InAdapter::getDouble, DataTypes::setDouble),
    date(InAdapter::getDate, DataTypes::setDate),
    timestamp(InAdapter::getTimestamp, DataTypes::setTimestamp),
    localDate(InAdapter::getLocalDate, DataTypes::setDate); //   LocalDate getLocalDate (String fieldName) throws Exception

    protected BiFunctionWithException<InAdapter, String, Object, Exception> getter;
    protected TriConsumerWithException<OutAdapter, String, Object, Exception> setter;

    DataTypes(BiFunctionWithException<InAdapter, String, Object, Exception> getter, TriConsumerWithException<OutAdapter, String, Object, Exception> setter) {
        this.getter = getter;
        this.setter = setter;
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

    public static void setString(OutAdapter adapter, String fieldName, Object value) throws Exception {
        if (value == null || value instanceof String)
            adapter.setString(fieldName, (String) value);
        else if (value instanceof ValueNode)
            adapter.setString(fieldName, ((TextNode)value).asText());
        else
            adapter.setString(fieldName, value.toString());
    }

    public static void setJson(OutAdapter adapter, String fieldName, Object value) throws Exception {
        if (value == null || value instanceof JsonNode)
            adapter.setJson(fieldName, (JsonNode) value);
        else if (value instanceof String)
            adapter.setJson(fieldName, MapperFactory.getObjectReader().readTree((String)value));
        else
            throw new SaveException("Attempting to set field " + fieldName + " of type JsonNode to value of type " + value.getClass().getName());
    }

    public static void setInt(OutAdapter adapter, String fieldName, Object value) throws Exception {
        if (value == null || value instanceof Integer)
            adapter.setInt(fieldName, (Integer) value);
        else if (value instanceof String)
            adapter.setInt(fieldName, Integer.valueOf((String)value));
        else if (value instanceof ValueNode)
            adapter.setInt(fieldName, ((ValueNode)value).asInt());
        else
            adapter.setInt(fieldName, ((Number)value).intValue());
    }

    public static void setBoolean(OutAdapter adapter, String fieldName, Object value) throws Exception {
        if (value == null || value instanceof Boolean)
            adapter.setBoolean(fieldName, (Boolean) value);
        else if (value instanceof String) {
            switch (((String)value).toUpperCase()){
                case "TRUE" :
                case "YES" :
                case "Y" :
                case "ON" : adapter.setBoolean(fieldName, true); break;
                default :
                    adapter.setBoolean(fieldName, false);
            }
        }else if (value instanceof Number)
            adapter.setBoolean(fieldName, ((Number)value).intValue() > 0);
        else if (value instanceof ValueNode)
            adapter.setBoolean(fieldName, ((ValueNode)value).asBoolean());
        else
            throw new SaveException("Attempting to set field " + fieldName + " of type Boolean to value of type " + value.getClass().getName());
    }

    public static void setLong(OutAdapter adapter, String fieldName, Object value) throws Exception {
        if (value == null || value instanceof Long)
            adapter.setLong(fieldName, (Long) value);
        else if (value instanceof String)
            adapter.setLong(fieldName, Long.valueOf((String)value));
        else if (value instanceof ValueNode)
            adapter.setLong(fieldName, ((ValueNode)value).asLong());
        else
            adapter.setLong(fieldName, ((Number)value).longValue());
    }

    public static void setDouble(OutAdapter adapter, String fieldName, Object value) throws Exception {
        if (value == null || value instanceof Double)
            adapter.setDouble(fieldName, (Double) value);
        else if (value instanceof String)
            adapter.setDouble(fieldName, Double.valueOf((String)value));
        else if (value instanceof ValueNode)
            adapter.setDouble(fieldName, ((ValueNode)value).asDouble());
        else
            adapter.setDouble(fieldName, ((Number)value).doubleValue());
    }

    public static void setDate(OutAdapter adapter, String fieldName, Object value) throws Exception {
        if (value == null || value instanceof Date)
            adapter.setDate(fieldName, (Date) value);
        else if (value instanceof LocalDate)
            adapter.setDate(fieldName, Date.valueOf((LocalDate) value));
        else if (value instanceof ValueNode)
            adapter.setDate(fieldName, JsonInputAdapter.jsonNodeToDate((JsonNode)value));
        else if (value instanceof String)
            adapter.setDate(fieldName, Date.valueOf((String)value));
        else if (value instanceof Timestamp)
            adapter.setDate(fieldName, new Date(((Timestamp)value).getTime()));
        else
            throw new SaveException("Attempting to set field " + fieldName + " of type java.sql.Date to value of type " + value.getClass().getName());
    }

   public static void setTimestamp(OutAdapter adapter, String fieldName, Object value) throws Exception {
        if (value == null || value instanceof Timestamp)
            adapter.setTimestamp(fieldName, (Timestamp) value);
        else if (value instanceof LocalDate)
            adapter.setTimestamp(fieldName, new Timestamp(Date.valueOf((LocalDate) value).getTime()));
        else if (value instanceof String)
            adapter.setTimestamp(fieldName, Timestamp.valueOf((String)value));
        else if (value instanceof ValueNode)
            adapter.setTimestamp(fieldName, Timestamp.valueOf(((ValueNode)value).asText()));
        else if (value instanceof LocalDateTime)
            adapter.setTimestamp(fieldName, Timestamp.valueOf((LocalDateTime)value));
        else if (value instanceof Date)
            adapter.setTimestamp(fieldName, new Timestamp(((Date)value).getTime()));
        else
            throw new SaveException("Attempting to set field " + fieldName + " of type java.sql.Timestamp to value of type " + value.getClass().getName());
    }

 }
