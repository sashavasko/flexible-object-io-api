package org.sv.flexobject.sql;

import com.fasterxml.jackson.databind.JsonNode;
import org.sv.flexobject.InAdapter;
import org.sv.flexobject.copy.CopyAdapter;
import org.sv.flexobject.copy.Copyable;
import org.sv.flexobject.json.MapperFactory;

import java.sql.*;
import java.util.Map;

public class SqlInputAdapter implements InAdapter, Copyable {

    public enum PARAMS {
        preparedStatement,
        resultSet
    }

    PreparedStatement preparedStatement;
    ResultSet resultSet;

    public SqlInputAdapter() {
    }

    public SqlInputAdapter(ResultSet resultSet, PreparedStatement preparedStatement) {
        this.resultSet = resultSet;
        this.preparedStatement = preparedStatement;
    }

    int getFieldIndex(String fieldName){
        try{
            return resultSet.findColumn(translateInputFieldName(fieldName));
        } catch (SQLException e) {
            return -1;
        }
    }

    @Override
    public String getString(String fieldName) throws SQLException {
        int idx = getFieldIndex(fieldName);
        if (idx < 0)
            return null;
        return resultSet.getString(idx);
    }

    @Override
    public JsonNode getJson(String fieldName) throws Exception {
        String jsonString = getString(fieldName);
        if (jsonString == null)
            return null;

        return MapperFactory.getObjectReader().readTree(jsonString);
    }

    @Override
    public Integer getInt(String fieldName) throws SQLException {
        int idx = getFieldIndex(fieldName);
        if (idx < 0)
            return null;

        return resultSet.getInt(idx);
    }

    @Override
    public Boolean getBoolean(String fieldName) throws Exception {
        int idx = getFieldIndex(fieldName);
        if (idx < 0)
            return null;

        return resultSet.getBoolean(idx);
    }

    @Override
    public Long getLong(String fieldName) throws SQLException {
        int idx = getFieldIndex(fieldName);
        if (idx < 0)
            return null;

        return resultSet.getLong(idx);
    }

    @Override
    public Double getDouble(String fieldName) throws SQLException {
        int idx = getFieldIndex(fieldName);
        if (idx < 0)
            return null;

        return resultSet.getDouble(idx);
    }

    @Override
    public Date getDate(String fieldName) throws SQLException {
        int idx = getFieldIndex(fieldName);
        if (idx < 0)
            return null;

        return resultSet.getDate(idx);
    }

    @Override
    public Timestamp getTimestamp(String fieldName) throws SQLException {
        int idx = getFieldIndex(fieldName);
        if (idx < 0)
            return null;

        try {
            return resultSet.getTimestamp(idx);
            // the following is needed when zeroDateTimeBehavior=convertToNull is not used in MySQL connection url
            // gradle requires that : compile group: 'mysql', name: 'mysql-connector-java', version: '8.0.15'
        } catch (SQLException e) {
//            if (e.getCause() instanceof DataReadException) {
            if ("Zero date value prohibited".equals(e.getMessage())
                    || (e.getCause() != null && "Zero date value prohibited".equals(e.getCause().getMessage()))) {
                return null;
            } else {
                throw e;
            }
        }
    }

    @Override
    public boolean next() throws Exception {
        return resultSet.next();
    }

    @Override
    public void close() throws Exception {
        resultSet.close();
        preparedStatement.close();
    }

    @Override
    public void copyRecord(CopyAdapter to) throws Exception {
        ResultSetMetaData md = resultSet.getMetaData();
        for (int i = 1 ; i <= md.getColumnCount() ; ++i){
            String colName = md.getColumnName(i);
            Object value = resultSet.getObject(i);
            if (value != null)
                to.put(colName, value);
        }
    }

    @Override
    public void setParam(String key, Object value) {
        setParam(PARAMS.valueOf(key), value);
    }

    public void setParam(PARAMS key, Object value) {
        if (key == PARAMS.preparedStatement && value != null && value instanceof PreparedStatement)
            preparedStatement = (PreparedStatement) value;
        else if (key == PARAMS.resultSet && value != null && value instanceof ResultSet)
            resultSet = (ResultSet) value;
    }
}
