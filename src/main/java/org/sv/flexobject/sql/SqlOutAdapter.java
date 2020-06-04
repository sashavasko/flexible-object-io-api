package org.sv.flexobject.sql;

import com.fasterxml.jackson.databind.JsonNode;
import org.sv.flexobject.OutAdapter;
import org.sv.flexobject.json.MapperFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class SqlOutAdapter implements OutAdapter {

    protected PreparedStatement preparedStatement = null;
    protected Map<String, Integer> paramNamesXref;
    protected int setParametersCount = 0;

    public static Map<String, Integer> buildParamNameXref(String ... args){
        int paramIdx = 1;
        Map<String, Integer> xref = new HashMap<>();
        for (String param : args){
            xref.put(param, paramIdx++);
        }
        return xref;
    }

    public SqlOutAdapter() {
    }

    public SqlOutAdapter(Map<String, Integer> paramNamesXref) {
        this(null, paramNamesXref);
    }

    public SqlOutAdapter(PreparedStatement st, Map<String, Integer> paramNamesXref) {
        this.preparedStatement = st;
        this.paramNamesXref = paramNamesXref;
    }

    public SqlOutAdapter setPreparedStatement(PreparedStatement preparedStatement) throws Exception {
        this.preparedStatement = preparedStatement;
        return this;
    }

    protected int getParamIndex(String paramName){
        paramName = translateOutputFieldName(paramName);
        return paramNamesXref.containsKey(paramName) ? paramNamesXref.get(paramName) : -1;
    }

    @Override
    public void setString(String paramName, String value) throws SQLException {
        int paramIdx = getParamIndex(paramName);
        if (paramIdx >= 0) {
            if (value != null)
                preparedStatement.setString(paramIdx, value);
            else
                preparedStatement.setNull(paramIdx, Types.VARCHAR);
            setParametersCount++;
        }
    }

    @Override
    public void setJson(String paramName, JsonNode value) throws Exception {
        if (value != null)
            setString(paramName, MapperFactory.getObjectWriter().writeValueAsString(value));
        else
            setString(paramName, null);
        setParametersCount++;
    }

    @Override
    public void setInt(String paramName, Integer value) throws SQLException {
        int paramIdx = getParamIndex(paramName);
        if (paramIdx >= 0) {
            if (value != null)
                preparedStatement.setInt(paramIdx, value);
            else
                preparedStatement.setNull(paramIdx, Types.INTEGER);
            setParametersCount++;
        }
    }

    @Override
    public void setBoolean(String paramName, Boolean value) throws Exception {
        int paramIdx = getParamIndex(paramName);
        if (paramIdx >= 0) {
            if (value != null)
                preparedStatement.setBoolean(paramIdx, value);
            else
                preparedStatement.setNull(paramIdx, Types.BOOLEAN);
            setParametersCount++;
        }
    }

    @Override
    public void setLong(String paramName, Long value) throws SQLException {
        int paramIdx = getParamIndex(paramName);
        if (paramIdx >= 0) {
            if (value != null)
                preparedStatement.setLong(paramIdx, value);
            else
                preparedStatement.setNull(paramIdx, Types.BIGINT);
            setParametersCount++;
        }
    }

    @Override
    public void setDate(String paramName, Date value) throws SQLException {
        int paramIdx = getParamIndex(paramName);
        if (paramIdx >= 0) {
            if (value != null)
                preparedStatement.setDate(paramIdx, value);
            else
                preparedStatement.setNull(paramIdx, Types.DATE);
            setParametersCount++;
        }
    }

    @Override
    public void setTimestamp(String paramName, Timestamp value) throws SQLException {
        int paramIdx = getParamIndex(paramName);
        if (paramIdx >= 0) {
            if (value != null) {
                // This sucks, but that's the only way to get millisecond precision through the MySQL jdbc driver
                preparedStatement.setString(paramIdx, value.toString());
            }else
                preparedStatement.setNull(paramIdx, Types.TIMESTAMP);
            setParametersCount++;
        }
    }

    protected void clearParameters() throws Exception {
        preparedStatement.clearParameters();
        setParametersCount = 0;
    }

    @Override
    public SqlOutAdapter save() throws Exception {
        int rowsAffected = preparedStatement.executeUpdate();
        clearParameters();
        if (rowsAffected == 0)
            throw new RuntimeException("Failed to save record - 0 rows affected.");
        return this;
    }

    @Override
    public boolean shouldSave() {
        return setParametersCount > 0;
    }

    @Override
    public void setParam(String key, Object value) {
        if ("paramNamesXref".equals(key) && value != null && value instanceof Map)
            paramNamesXref = (Map<String, Integer>) value;
        else if ("preparedStatement".equals(key) && value != null && value instanceof PreparedStatement)
            preparedStatement = (PreparedStatement) value;
    }
}
