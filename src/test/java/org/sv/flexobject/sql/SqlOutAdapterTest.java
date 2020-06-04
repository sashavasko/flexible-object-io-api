package org.sv.flexobject.sql;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.sql.*;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class SqlOutAdapterTest {

    @Mock
    PreparedStatement st;

    SqlOutAdapter adapter;

    Map<String, Integer> paramNamesXref = new HashMap();

    @Before
    public void setUp() throws Exception {
        paramNamesXref.put("field", 1);
        paramNamesXref.put("anotherfield", 2);
        adapter = new SqlOutAdapter(st, paramNamesXref);
    }

    @Test
    public void setPreparedStatement() throws Exception {
        adapter = new SqlOutAdapter(paramNamesXref);
        adapter.setPreparedStatement(st);

        assertEquals(st, adapter.preparedStatement);

    }

    @Test
    public void getParamIndex() {
        assertEquals(1, adapter.getParamIndex("field"));
        assertEquals(2, adapter.getParamIndex("anotherfield"));
    }

    @Test
    public void setStringNull() throws SQLException {
        adapter.setString("field", null);
        verify(st).setNull(1, Types.VARCHAR);
    }

    @Test
    public void setStringBadField() throws SQLException {
        adapter.setString("badfield", "Yes");
        verifyNoMoreInteractions(st);
    }

    @Test
    public void setString() throws SQLException {
        adapter.setString("field", "yes");
        verify(st).setString(1, "yes");
    }

    @Test
    public void setJson() throws Exception {
        ObjectNode top = JsonNodeFactory.instance.objectNode();
        JsonNode value = JsonNodeFactory.instance.numberNode(100);
        top.set("foo", value);
        adapter.setJson("field", top);
        verify(st).setString(1, "{\"foo\":100}");
    }

    @Test
    public void setInt() throws SQLException {
        adapter.setInt("field", 1);
        verify(st).setInt(1, 1);
    }

    @Test
    public void setIntForNull() throws SQLException {
        adapter.setInt("field", null);
        verify(st).setNull(1, Types.INTEGER);
    }

    @Test
    public void setIntForBadParameter() throws SQLException {
        adapter.setInt("tooManyFields", 1);
        verifyZeroInteractions(st);
    }

    @Test
    public void setBoolean() throws Exception {
        adapter.setBoolean("field", true);
        verify(st).setBoolean(1, true);
    }

    @Test
    public void setBooleanNullValue() throws Exception {
        adapter.setBoolean("field", null);
        verify(st).setNull(1, Types.BOOLEAN);
    }

    @Test
    public void setBooleanBadParam() throws Exception {
        adapter.setBoolean("badfield", true);
        verifyZeroInteractions(st);
    }

    @Test
    public void setLong() throws SQLException  {
        adapter.setLong("field", 1l);
        verify(st).setLong(1, 1l);
    }

    @Test
    public void setLongForNull() throws SQLException  {
        adapter.setLong("field", null);
        verify(st).setNull(1, Types.BIGINT);
    }

    @Test
    public void setLongForBadParam() throws SQLException  {
        adapter.setLong("badfield", 1l);
        verifyZeroInteractions(st);
    }

    @Test
    public void setDate()  throws SQLException {
        Date date = new Date(1234567l);
        adapter.setDate("field", date);
        verify(st).setDate(1, date);
    }

    @Test
    public void setDateNullValue() throws Exception {
        adapter.setDate("field", (LocalDate)null);

        verify(st).setNull(1, Types.DATE);
    }

    @Test
    public void setDateBadField()  throws SQLException {
        Date date = new Date(1234567l);
        adapter.setDate("badfield", date);
        verifyZeroInteractions(st);
    }

    @Test
    public void setTimestamp()  throws SQLException {
        Timestamp timestamp = new Timestamp(1234567l);
        adapter.setTimestamp("field", timestamp);
//        verify(st).setTimestamp(1, timestamp);
        // Workaround for a bug in MySQL driver resulting in tructation of second fraction :
        verify(st).setString(1, timestamp.toString());
    }

    @Test
    public void setTimestampNullValue()  throws SQLException {
        adapter.setTimestamp("field", null);
        verify(st).setNull(1, Types.TIMESTAMP);
    }

    @Test
    public void setTimestampBadField()  throws SQLException {
        Timestamp timestamp = new Timestamp(1234567l);
        adapter.setTimestamp("badfield", timestamp);
        verifyZeroInteractions(st);
    }

    @Test
    public void save() throws Exception {
        Mockito.when(st.executeUpdate()).thenReturn(1);
        adapter.save();
        verify(st).clearParameters();
    }
}