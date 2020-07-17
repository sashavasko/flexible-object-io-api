package org.sv.flexobject;

import com.fasterxml.jackson.databind.JsonNode;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;

public interface OutAdapter extends Parameterized{

    // WARNING: Output adapters are not thread safe and cannot possibly be
    default boolean supportField(String fieldName) {return true;}
    void setString(String paramName, String value) throws Exception;
    void setJson(String paramName, JsonNode value) throws Exception;
    void setInt(String paramName, Integer value) throws Exception;
    void setBoolean(String paramName, Boolean value) throws Exception;
    void setLong(String paramName, Long value) throws Exception;
    void setDouble(String paramName, Double value) throws Exception;
    void setDate(String paramName, Date value) throws Exception;
    default void setDate(String paramName, LocalDate value) throws Exception{
        setDate(paramName, value == null ? null : Date.valueOf(value));
    }
    void setTimestamp(String paramName, Timestamp value) throws Exception;

    OutAdapter save() throws Exception;
    boolean shouldSave();

    default boolean saveIfYouShould() throws Exception {
        if (!shouldSave())
            return false;
        save();
        return true;
    }

    default String translateOutputFieldName(String fieldName)
    {
        return fieldName;
    }

}
