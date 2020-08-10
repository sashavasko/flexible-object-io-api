package org.sv.flexobject.sql;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.sv.flexobject.copy.CopyAdapter;
import org.sv.flexobject.json.MapperFactory;

import java.io.IOException;
import java.sql.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class SqlInputAdapterTest {

    @Mock
    ResultSet rs;

    @Mock
    PreparedStatement ps;

    @Mock
    ResultSetMetaData resultSetMetaData;

    @Mock
    CopyAdapter copyAdapter;

    SqlInputAdapter adapter;

    @Before
    public void setUp() throws Exception {
        adapter = new SqlInputAdapter(rs, ps);
        when(rs.findColumn("boo")).thenReturn(1);
        when(rs.findColumn("foo")).thenReturn(-1);

        when(rs.getMetaData()).thenReturn(resultSetMetaData);
    }

    @Test
    public void unknownField() throws SQLException {
        when(rs.findColumn("unknown")).thenThrow(new SQLException());

        assertEquals(-1, adapter.getFieldIndex("unknown"));
    }

    @Test
    public void getString() throws SQLException {
        when(rs.getString(1)).thenReturn("Pika");
        assertEquals("Pika", adapter.getString("boo"));

        assertNull(adapter.getString("foo"));
    }

    @Test
    public void getInt() throws SQLException {
        when(rs.getInt(1)).thenReturn(29);
        assertEquals(29, (int)adapter.getInt("boo"));

        assertNull(adapter.getInt("foo"));
    }

    @Test
    public void getBoolean() throws Exception {
        when(rs.getBoolean(1)).thenReturn(true);
        assertTrue( adapter.getBoolean("boo"));

        assertNull(adapter.getBoolean("foo"));
    }

    @Test
    public void getLong() throws SQLException {
        when(rs.getLong(1)).thenReturn(29l);
        assertEquals(29l, (long)adapter.getLong("boo"));

        assertNull(adapter.getLong("foo"));
    }

    @Test
    public void getDate() throws SQLException {
        Date date = new Date(1234567l);
        when(rs.getDate(1)).thenReturn(date);
        assertEquals(date, adapter.getDate("boo"));

        assertNull(adapter.getDate("foo"));
    }

    @Test
    public void getTimestamp() throws SQLException {
        Timestamp timestamp = new Timestamp(1234567l);
        when(rs.getTimestamp(1)).thenReturn(timestamp);
        assertEquals(timestamp, adapter.getTimestamp("boo"));

        assertNull(adapter.getTimestamp("foo"));
    }

    @Test
    public void getZeroTimestamp() throws SQLException {
        SQLException exception = new SQLException("Zero date value prohibited");

        when(rs.getTimestamp(1)).thenThrow(exception);

        assertNull(adapter.getTimestamp("boo"));
    }

    @Test(expected = SQLException.class)
    public void getZeroTimestampButOnlyForZeroDateFoo() throws SQLException {
        SQLException exception = new SQLException("foo");
        when(rs.getTimestamp(1)).thenThrow(exception);

        adapter.getTimestamp("boo");
    }

    @Test
    public void getZeroTimestampButOnlyForZeroDateBar() throws SQLException {
        SQLException exception = new SQLException(new RuntimeException("Zero date value prohibited"));
        when(rs.getTimestamp(1)).thenThrow(exception);

        assertNull(adapter.getTimestamp("boo"));
    }

    @Test
    public void next() throws Exception {
        adapter.next();
        verify(rs).next();
    }

    @Test
    public void getJson() throws Exception {
        String jsonString = "{'a':'tango', 'b':'bravo'}".replace('\'', '"');
        when(rs.getString(1)).thenReturn(jsonString);
        assertEquals(MapperFactory.getObjectReader().readTree(jsonString), adapter.getJson("boo"));

        assertNull(adapter.getJson("foo"));
    }

    @Test
    public void close() throws Exception {
        adapter.close();
        verify(rs).close();
        verify(ps).close();
    }

    @Test
    public void setParam() throws Exception {
        SqlInputAdapter adapter = new SqlInputAdapter();
        adapter.setParam(SqlInputAdapter.PARAMS.preparedStatement.name(), ps);
        adapter.setParam(SqlInputAdapter.PARAMS.resultSet.name(), rs);
        adapter.close();
        verify(rs).close();
        verify(ps).close();
    }

    @Test
    public void copyRecord() throws Exception {
        when(resultSetMetaData.getColumnCount()).thenReturn(2);
        when(resultSetMetaData.getColumnName(1)).thenReturn("foo");
        when(resultSetMetaData.getColumnName(2)).thenReturn("bar");
        when(rs.getObject(1)).thenReturn("tango");
        when(rs.getObject(2)).thenReturn("bravo");

        adapter.copyRecord(copyAdapter);

        verify(copyAdapter).put("foo", "tango");
        verify(copyAdapter).put("bar", "bravo");
        verifyNoMoreInteractions(copyAdapter);

    }
}