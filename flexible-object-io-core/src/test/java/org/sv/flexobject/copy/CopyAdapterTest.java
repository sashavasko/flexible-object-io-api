package org.sv.flexobject.copy;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.sql.Timestamp;
import java.sql.Date;
import java.util.Set;

import static org.junit.Assert.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class CopyAdapterTest {

    @Mock
    Set fieldsOut;

    @Mock
    Set fieldsIn;

    @Mock
    JsonNode mockJson;

    @Mock
    Date mockDate;

    @Mock
    Timestamp mockTimestamp;

    @Mock
    CopyAdapter mockCopyAdapter;

    CopyAdapter adapter = new CopyAdapter();

    @Test
    public void allowedFieldsPutAndGet() {
        adapter.setAllowedOutputFields(fieldsOut);

        adapter.put("a", "foo");
        assertFalse(adapter.containsKey("a"));

        when(fieldsOut.contains("a")).thenReturn(true);

        adapter.put("a", "foo");

        assertTrue(adapter.containsKey("a"));

        adapter.setAllowedInputFields(fieldsIn);
        assertNull(adapter.get("a"));

        when(fieldsIn.contains("a")).thenReturn(true);
        assertEquals("foo", adapter.get("a"));
    }

    @Test
    public void getString() throws Exception {
        adapter.put("field", "bar");
        adapter.setString("fieldSet", "foo");
        adapter.put("fieldLong", 1234567l);

        assertEquals("bar", adapter.getString("field"));
        assertEquals("foo", adapter.getString("fieldSet"));
        assertEquals("1234567", adapter.getString("fieldLong"));
        assertNull(adapter.getString("badfield"));
    }

    @Test
    public void getJson() throws Exception {
        adapter.put("field", mockJson);
        adapter.setJson("fieldSet", mockJson);

        assertSame(mockJson, adapter.getJson("field"));
        assertSame(mockJson, adapter.getJson("fieldSet"));
        assertNull(adapter.getJson("badfield"));
    }

    @Test
    public void getInt() throws Exception {
        adapter.put("field", 777);
        adapter.setInt("fieldSet", 7777);
        adapter.put("fieldLong", 777999999l);

        assertEquals(777, (int)adapter.getInt("field"));
        assertEquals(7777, (int)adapter.getInt("fieldSet"));
        assertEquals(((Number)777999999l).intValue(), (int)adapter.getInt("fieldLong"));
        assertNull(adapter.getInt("badfield"));
    }

    @Test
    public void getBoolean() throws Exception {
        adapter.put("field", true);
        adapter.setBoolean("fieldSet", true);
        adapter.put("fieldInt", 1);
        adapter.put("fieldIntFalse", 0);
        adapter.put("fieldString", "true");
        adapter.put("fieldStringY", "y");
        adapter.put("fieldStringYes", "yes");
        adapter.put("fieldStringNo", "no");
        adapter.put("fieldStringFalse", "false");

        assertTrue(adapter.getBoolean("field"));
        assertTrue(adapter.getBoolean("fieldSet"));
        assertTrue(adapter.getBoolean("fieldInt"));
        assertFalse(adapter.getBoolean("fieldIntFalse"));
        assertTrue(adapter.getBoolean("fieldString"));
        assertTrue(adapter.getBoolean("fieldStringY"));
        assertTrue(adapter.getBoolean("fieldStringYes"));
        assertFalse(adapter.getBoolean("fieldStringNo"));
        assertFalse(adapter.getBoolean("fieldStringFalse"));
        assertNull(adapter.getBoolean("badfield"));
    }

    @Test
    public void getLong() throws Exception {
        adapter.put("field", 777);
        adapter.setLong("fieldSet", 7777l);
        adapter.put("fieldLong", 777999999l);

        assertEquals(777l, (long)adapter.getLong("field"));
        assertEquals(7777l, (long)adapter.getLong("fieldSet"));
        assertEquals(777999999l, (long)adapter.getLong("fieldLong"));
        assertNull(adapter.getLong("badfield"));
    }

    @Test
    public void getDate() throws Exception {
        adapter.put("field", mockDate);
        adapter.setDate("fieldSet", mockDate);

        assertSame(mockDate, adapter.getDate("field"));
        assertSame(mockDate, adapter.getDate("fieldSet"));
        assertNull(adapter.getDate("badfield"));
    }

    @Test
    public void getTimestamp() throws Exception {
        adapter.put("field", mockTimestamp);
        adapter.setTimestamp("fieldSet", mockTimestamp);

        assertSame(mockTimestamp, adapter.getTimestamp("field"));
        assertSame(mockTimestamp, adapter.getTimestamp("fieldSet"));
        assertNull(adapter.getTimestamp("badfield"));
    }

    @Test
    public void next() throws Exception {
        assertTrue(adapter.next());
    }

    @Test
    public void save() throws Exception {
        assertSame(adapter, adapter.save());
    }

    @Test
    public void shouldSave() {
        assertTrue(adapter.shouldSave());
    }

    @Test
    public void close() throws Exception {
        adapter.close();
    }

    @Test
    public void copyRecord() throws Exception {
        adapter.copyRecord(mockCopyAdapter);

        verify(mockCopyAdapter).putAll(adapter);
    }

    @Test
    public void setParam() {
        adapter.setParam(CopyAdapter.PARAMS.allowedOutputFields.name(), fieldsOut);

        adapter.put("a", "foo");
        assertFalse(adapter.containsKey("a"));

        when(fieldsOut.contains("a")).thenReturn(true);

        adapter.put("a", "foo");

        assertTrue(adapter.containsKey("a"));

        adapter.setParam(CopyAdapter.PARAMS.allowedInputFields.name(), fieldsIn);
        assertNull(adapter.get("a"));

        when(fieldsIn.contains("a")).thenReturn(true);
        assertEquals("foo", adapter.get("a"));
    }
}