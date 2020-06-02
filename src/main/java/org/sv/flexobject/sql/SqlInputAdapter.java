package org.sv.flexobject.sql;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.sv.flexobject.InAdapter;
import org.sv.flexobject.copy.CopyAdapter;
import org.sv.flexobject.copy.Copyable;

import java.sql.*;

public class SqlInputAdapter implements InAdapter, Copyable {

    ObjectMapper objectMapper;
    PreparedStatement ps;
    ResultSet rs;

    public SqlInputAdapter(ResultSet rs, PreparedStatement ps) {
        this.rs = rs;
        this.ps = ps;
    }

    int getFieldIndex(String fieldName){
        try{
            return rs.findColumn(translateInputFieldName(fieldName));
        } catch (SQLException e) {
            return -1;
        }
    }

    @Override
    public String getString(String fieldName) throws SQLException {
        int idx = getFieldIndex(fieldName);
        if (idx < 0)
            return null;
        return rs.getString(idx);
    }

    @Override
    public JsonNode getJson(String fieldName) throws Exception {
        String jsonString = getString(fieldName);
        if (jsonString == null)
            return null;
        if (objectMapper == null)
            objectMapper = new ObjectMapper();

        return objectMapper.readTree(jsonString);
    }

    @Override
    public Integer getInt(String fieldName) throws SQLException {
        int idx = getFieldIndex(fieldName);
        if (idx < 0)
            return null;

        return rs.getInt(idx);
    }

    @Override
    public Boolean getBoolean(String fieldName) throws Exception {
        int idx = getFieldIndex(fieldName);
        if (idx < 0)
            return null;

        return rs.getBoolean(idx);
    }

    @Override
    public Long getLong(String fieldName) throws SQLException {
        int idx = getFieldIndex(fieldName);
        if (idx < 0)
            return null;

        return rs.getLong(idx);
    }

    @Override
    public Date getDate(String fieldName) throws SQLException {
        int idx = getFieldIndex(fieldName);
        if (idx < 0)
            return null;

        return rs.getDate(idx);
    }

    @Override
    public Timestamp getTimestamp(String fieldName) throws SQLException {
        int idx = getFieldIndex(fieldName);
        if (idx < 0)
            return null;

        try {
            return rs.getTimestamp(idx);
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
        return rs.next();
    }

    @Override
    public void close() throws Exception {
        rs.close();
        ps.close();
    }

    @Override
    public void copyRecord(CopyAdapter to) throws Exception {
        ResultSetMetaData md = rs.getMetaData();
        for (int i = 1 ; i <= md.getColumnCount() ; ++i){
            String colName = md.getColumnName(i);
            Object value = rs.getObject(i);
            if (value != null)
                to.put(colName, value);
        }
    }
}
