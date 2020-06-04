package org.sv.flexobject.sql;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.sql.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SqlInputAdapterTest {

    @Mock
    ResultSet rs;

    @Mock
    PreparedStatement ps;

    SqlInputAdapter adapter;

    @Before
    public void setUp() throws Exception {
        adapter = new SqlInputAdapter(rs, ps);
        when(rs.findColumn("boo")).thenReturn(1);
    }

    @Test
    public void getString() throws SQLException {
        when(rs.getString(1)).thenReturn("Pika");
        assertEquals("Pika", adapter.getString("boo"));
    }

    @Test
    public void getInt() throws SQLException {
        when(rs.getInt(1)).thenReturn(29);
        assertEquals(29, (int)adapter.getInt("boo"));
    }

    @Test
    public void getBoolean() throws Exception {
        when(rs.getBoolean(1)).thenReturn(true);
        assertTrue( adapter.getBoolean("boo"));
    }

    @Test
    public void getLong() throws SQLException {
        when(rs.getLong(1)).thenReturn(29l);
        assertEquals(29l, (long)adapter.getLong("boo"));
    }

    @Test
    public void getDate() throws SQLException {
        Date date = new Date(1234567l);
        when(rs.getDate(1)).thenReturn(date);
        assertEquals(date, adapter.getDate("boo"));
    }

    @Test
    public void getTimestamp() throws SQLException {
        Timestamp timestamp = new Timestamp(1234567l);
        when(rs.getTimestamp(1)).thenReturn(timestamp);
        assertEquals(timestamp, adapter.getTimestamp("boo"));
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
}