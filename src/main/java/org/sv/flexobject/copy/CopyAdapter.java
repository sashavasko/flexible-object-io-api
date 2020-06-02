package org.sv.flexobject.copy;

import com.fasterxml.jackson.databind.JsonNode;
import org.sv.flexobject.InAdapter;
import org.sv.flexobject.OutAdapter;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Set;

public class CopyAdapter extends HashMap<String, Object> implements OutAdapter, InAdapter, Copyable {

    protected Set<String> allowedInputFields = null;
    protected Set<String> allowedOutputFields = null;

    public void setAllowedInputFields(Set<String> allowedInputFields) {
        this.allowedInputFields = allowedInputFields;
    }

    public void setAllowedOutputFields(Set<String> allowedOutputFields) {
        this.allowedOutputFields = allowedOutputFields;
    }

    @Override
    public Object put(String key, Object value) {
        String fieldName = translateOutputFieldName(key);
        if (allowedOutputFields == null || allowedOutputFields.contains(fieldName))
            return super.put(fieldName, value);
        return null;
    }

    @Override
    public Object get(Object key) {
        String fieldName = translateInputFieldName((String)key);
        if (allowedInputFields == null || allowedInputFields.contains(fieldName))
            return super.get(fieldName);
        return null;
    }

    @Override
    public String getString(String fieldName) throws Exception {
        Object value = get(fieldName);
        if (value == null)
            return null;

        if (value instanceof String)
            return (String) value;
        return value.toString();
    }

    @Override
    public JsonNode getJson(String fieldName) throws Exception {
        return (JsonNode) get(fieldName);
    }

    @Override
    public Integer getInt(String fieldName) throws Exception {
        return (Integer) get(fieldName);
    }

    @Override
    public Boolean getBoolean(String fieldName) throws Exception {
        return (Boolean) get(fieldName);
    }

    @Override
    public Long getLong(String fieldName) throws Exception {
        return (Long) get(fieldName);
    }

    @Override
    public Date getDate(String fieldName) throws Exception {
        return (Date) get(fieldName);
    }

    @Override
    public Timestamp getTimestamp(String fieldName) throws Exception {
        return (Timestamp) get(fieldName);
    }

    @Override
    public boolean next() throws Exception {
        return true;
    }

    @Override
    public void setString(String paramName, String value) throws Exception {
        put(paramName, value);
    }

    @Override
    public void setJson(String paramName, JsonNode value) throws Exception {
        put(paramName, value);
    }

    @Override
    public void setInt(String paramName, Integer value) throws Exception {
        put(paramName, value);
    }

    @Override
    public void setBoolean(String paramName, Boolean value) throws Exception {
        put(paramName, value);
    }

    @Override
    public void setLong(String paramName, Long value) throws Exception {
        put(paramName, value);
    }

    @Override
    public void setDate(String paramName, Date value) throws Exception {
        put(paramName, value);
    }

    @Override
    public void setTimestamp(String paramName, Timestamp value) throws Exception {
        put(paramName, value);
    }

    @Override
    public OutAdapter save() throws Exception {
        return this;
    }

    @Override
    public boolean shouldSave() {
        return true;
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public void copyRecord(CopyAdapter to) throws Exception {
        to.putAll(this);
    }
}
