package org.sv.flexobject.adapter;

import com.fasterxml.jackson.databind.JsonNode;
import org.sv.flexobject.OutAdapter;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;

public interface DynamicOutAdapter extends OutAdapter {

    /*
     * Returns value on success - same semantics as java.util.Map.put()
     */
    Object put(String fieldName, Object value) throws Exception;

    @Override
    default boolean supportField(String fieldName) {
        return true;
    }

    @Override
    default void setString(String paramName, String value) throws Exception{
        put(translateOutputFieldName(paramName), value);
    }

    @Override
    default void setJson(String paramName, JsonNode value) throws Exception{
        put(translateOutputFieldName(paramName), value);
    }

    @Override
    default void setInt(String paramName, Integer value) throws Exception{
        put(translateOutputFieldName(paramName), value);
    }

    @Override
    default void setBoolean(String paramName, Boolean value) throws Exception{
        put(translateOutputFieldName(paramName), value);
    }

    @Override
    default void setLong(String paramName, Long value) throws Exception{
        put(translateOutputFieldName(paramName), value);
    }

    @Override
    default void setDouble(String paramName, Double value) throws Exception{
        put(translateOutputFieldName(paramName), value);
    }

    @Override
    default void setDate(String paramName, Date value) throws Exception{
        put(translateOutputFieldName(paramName), value);
    }

    @Override
    default void setDate(String paramName, LocalDate value) throws Exception {
        put(translateOutputFieldName(paramName), value);
    }

    @Override
    default void setTimestamp(String paramName, Timestamp value) throws Exception{
        put(translateOutputFieldName(paramName), value);
    }
}
