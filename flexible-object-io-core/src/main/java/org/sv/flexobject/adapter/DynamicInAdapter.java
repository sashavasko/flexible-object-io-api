package org.sv.flexobject.adapter;

import com.fasterxml.jackson.databind.JsonNode;
import org.sv.flexobject.InAdapter;
import org.sv.flexobject.schema.DataTypes;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.Set;

public interface DynamicInAdapter extends InAdapter {

    Object get(Object fieldName);

    @Override
    default String getString(String fieldName) throws Exception{
        return DataTypes.stringConverter(get(fieldName));
    }

    @Override
    default JsonNode getJson(String fieldName) throws Exception{
        return DataTypes.jsonConverter(get(fieldName));
    }

    @Override
    default Integer getInt(String fieldName) throws Exception{
        return DataTypes.int32Converter(get(fieldName));
    }

    @Override
    default Long getLong(String fieldName) throws Exception{
        return DataTypes.int64Converter(get(fieldName));
    }

    @Override
    default Double getDouble(String fieldName) throws Exception{
        return DataTypes.float64Converter(get(fieldName));
    }

    @Override
    default Date getDate(String fieldName) throws Exception{
        return DataTypes.dateConverter(get(fieldName));
    }

    @Override
    default Timestamp getTimestamp(String fieldName) throws Exception {
        return DataTypes.timestampConverter(get(fieldName));
    }

    @Override
    default LocalDate getLocalDate(String fieldName) throws Exception {
        return DataTypes.localDateConverter(get(fieldName));
    }

    @Override
    default Boolean getBoolean(String fieldName) throws Exception {
        return DataTypes.boolConverter(get(fieldName));
    }

    @Override
    default Class<?> getClass(String fieldName) throws Exception {
        return DataTypes.classConverter(get(fieldName));
    }

    @Override
    default <T extends Enum<T>> T getEnum(String fieldName, T defaultValue) throws Exception {
        return DataTypes.enumConverter(get(fieldName), defaultValue);
    }

    @Override
    default Enum getEnum(String fieldName, Class<? extends Enum> enumClass) throws Exception {
        return DataTypes.enumConverter(get(fieldName), enumClass);
    }

    @Override
    default Set<Enum> getEnumSet(String fieldName, Class<? extends Enum> enumClass, String emptyValue) throws Exception {
        return DataTypes.enumSetConverter(get(fieldName), enumClass, emptyValue);
    }
}
