package org.sv.flexobject.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.junit.After;
import org.junit.Test;
import org.sv.flexobject.SaveException;
import org.sv.flexobject.copy.CopyAdapter;
import org.sv.flexobject.json.MapperFactory;

import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class DataTypesTest {

    CopyAdapter adapter = new CopyAdapter();

    LocalDate testLocalDate = LocalDate.of(2020,5,7);
    Date testDate = Date.valueOf(testLocalDate);
    Timestamp testTimestamp = new Timestamp(testDate.getTime());

    @After
    public void tearDown() throws Exception {
        adapter.clear();
    }

    @Test
    public void get() {
    }

    @Test
    public void getWithDefault() {
    }

    @Test
    public void set() {
    }

    @Test
    public void setString() throws Exception {
        DataTypes.setString(adapter, "foo", null);
        assertNull(adapter.get("foo"));

        adapter.clear();

        DataTypes.setString(adapter, "foo", "blah");
        assertEquals("blah", adapter.get("foo"));

        adapter.clear();

        DataTypes.setString(adapter, "foo", 12345);
        assertEquals("12345", adapter.get("foo"));

        adapter.clear();

        DataTypes.setString(adapter, "foo", JsonNodeFactory.instance.textNode("sometext"));
        assertEquals("sometext", adapter.get("foo"));
    }

    @Test
    public void setJson() throws Exception {
        DataTypes.setJson(adapter, "foo", null);
        assertNull(adapter.get("foo"));

        adapter.clear();

        String jsonString = "{'a':'foo', 'b':'bar', 'c':{'d':true,'e':1234567}}".replace("'", "\"");
        JsonNode json = MapperFactory.getObjectReader().readTree(jsonString);

        DataTypes.setJson(adapter, "jsonField", jsonString);
        assertEquals(json, adapter.getJson("jsonField"));

        adapter.clear();

        DataTypes.setJson(adapter, "jsonField", json);
        assertEquals(json, adapter.getJson("jsonField"));
    }

    @Test
    public void setJsonThrowsOnWrongDatatype() throws Exception {
        DataTypes.setJson(adapter, "jsonField", 12345);
        assertEquals("12345", adapter.getJson("jsonField").toString());
    }

    @Test
    public void setInt() throws Exception {
        DataTypes.setInt(adapter, "foo", null);
        assertNull(adapter.get("foo"));

        adapter.clear();

        DataTypes.setInt(adapter, "intField", 1234567);
        assertEquals(1234567, (int)adapter.getInt("intField"));

        adapter.clear();

        DataTypes.setInt(adapter, "intField", "12345679");
        assertEquals(12345679, (int)adapter.getInt("intField"));

        adapter.clear();

        DataTypes.setInt(adapter, "intField", 1234567988888l);
        assertEquals(((Number)1234567988888l).intValue(), (int)adapter.getInt("intField"));

        adapter.clear();

        DataTypes.setInt(adapter, "intField", 1234567.988888);
        assertEquals(1234567, (int)adapter.getInt("intField"));

        adapter.clear();

        DataTypes.setInt(adapter, "intField", JsonNodeFactory.instance.numberNode(1234567.988888));
        assertEquals(1234567, (int)adapter.getInt("intField"));

        adapter.clear();

        DataTypes.setInt(adapter, "intField", JsonNodeFactory.instance.textNode("1234565.988888"));
        assertEquals(1234565, (int)adapter.getInt("intField"));

        adapter.clear();
    }

    @Test
    public void setBoolean() throws Exception {
        DataTypes.setBoolean(adapter, "foo", null);
        assertNull(adapter.get("foo"));

        adapter.clear();

        DataTypes.setBoolean(adapter, "boolField", true);
        assertTrue(adapter.getBoolean("boolField"));

        adapter.clear();

        DataTypes.setBoolean(adapter, "boolField", false);
        assertFalse(adapter.getBoolean("boolField"));

        adapter.clear();

        DataTypes.setBoolean(adapter, "boolField", 1234567);
        assertTrue(adapter.getBoolean("boolField"));

        adapter.clear();

        DataTypes.setBoolean(adapter, "boolField", "TRUE");
        assertTrue(adapter.getBoolean("boolField"));

        adapter.clear();

        DataTypes.setBoolean(adapter, "boolField", "FALSE");
        assertFalse(adapter.getBoolean("boolField"));

        adapter.clear();

        DataTypes.setBoolean(adapter, "boolField", "YES");
        assertTrue(adapter.getBoolean("boolField"));

        adapter.clear();

        DataTypes.setBoolean(adapter, "boolField", "NO");
        assertFalse(adapter.getBoolean("boolField"));

        adapter.clear();

        DataTypes.setBoolean(adapter, "boolField", "Y");
        assertTrue(adapter.getBoolean("boolField"));

        adapter.clear();

        DataTypes.setBoolean(adapter, "boolField", JsonNodeFactory.instance.booleanNode(true));
        assertTrue(adapter.getBoolean("boolField"));

        adapter.clear();

        DataTypes.setBoolean(adapter, "boolField", JsonNodeFactory.instance.textNode("true"));
        assertTrue(adapter.getBoolean("boolField"));

        adapter.clear();

        DataTypes.setBoolean(adapter, "boolField", JsonNodeFactory.instance.booleanNode(false));
        assertFalse(adapter.getBoolean("boolField"));

        adapter.clear();

        DataTypes.setBoolean(adapter, "boolField", "N");
        assertFalse(adapter.getBoolean("boolField"));

        adapter.clear();
    }

    @Test(expected = SchemaException.class)
    public void setBooleanThrowsOnWrongDatatype() throws Exception {
        DataTypes.setBoolean(adapter, "boolField", JsonNodeFactory.instance.arrayNode());
    }

    @Test
    public void setLong() throws Exception {
        DataTypes.setLong(adapter, "foo", null);
        assertNull(adapter.get("foo"));

        adapter.clear();

        DataTypes.setLong(adapter, "longField", 1234567);
        assertEquals(1234567, (long)adapter.getLong("longField"));

        adapter.clear();

        DataTypes.setLong(adapter, "longField", "12345679");
        assertEquals(12345679, (long)adapter.getLong("longField"));

        adapter.clear();

        DataTypes.setLong(adapter, "longField", 1234567988888l);
        assertEquals(1234567988888l, (long)adapter.getLong("longField"));

        adapter.clear();

        DataTypes.setLong(adapter, "longField", 1234567.988888);
        assertEquals(1234567, (long)adapter.getLong("longField"));

        adapter.clear();

        DataTypes.setLong(adapter, "longField", JsonNodeFactory.instance.numberNode(1234567988888l));
        assertEquals(1234567988888l, (long)adapter.getLong("longField"));

        adapter.clear();

        DataTypes.setLong(adapter, "longField", JsonNodeFactory.instance.textNode("1234565.988888"));
        assertEquals(1234565, (long)adapter.getLong("longField"));
    }

    @Test
    public void setDouble() throws Exception {
        DataTypes.setDouble(adapter, "foo", null);
        assertNull(adapter.get("foo"));

        adapter.clear();

        DataTypes.setDouble(adapter, "doubleField", 1234567.1234567);
        assertEquals(1234567.1234567d, (double)adapter.getDouble("doubleField"),0.0000001);

        adapter.clear();

        DataTypes.setDouble(adapter, "doubleField", "12345679.1234567");
        assertEquals(12345679.1234567, (double)adapter.getDouble("doubleField"), 0.0000001);

        adapter.clear();

        DataTypes.setDouble(adapter, "doubleField", 1234567988888l);
        assertEquals(1234567988888l, (double)adapter.getDouble("doubleField"), 0.0001);

        adapter.clear();

        DataTypes.setDouble(adapter, "doubleField", JsonNodeFactory.instance.numberNode(12345679.88888));
        assertEquals(12345679.88888, (double)adapter.getDouble("doubleField"), 0.00001);

        adapter.clear();

        DataTypes.setDouble(adapter, "doubleField", JsonNodeFactory.instance.textNode("1234565.988888"));
        assertEquals(1234565.988888, (double)adapter.getDouble("doubleField"), 0.000001);
    }

    @Test
    public void setDate() throws Exception {
        DataTypes.setDate(adapter, "foo", null);
        assertNull(adapter.get("foo"));

        adapter.clear();

        DataTypes.setDate(adapter, "dateField", testDate);
        assertEquals(testDate, adapter.getDate("dateField"));

        adapter.clear();

        DataTypes.setDate(adapter, "dateField", testDate.toString());
        assertEquals(testDate, adapter.getDate("dateField"));

        adapter.clear();

        DataTypes.setDate(adapter, "dateField", testLocalDate);
        assertEquals(testDate, adapter.getDate("dateField"));

        adapter.clear();
        System.out.println(testDate.toString());
        DataTypes.setDate(adapter, "dateField", JsonNodeFactory.instance.textNode("2020-05-07"));
        assertEquals(testDate, adapter.getDate("dateField"));

        adapter.clear();

        DataTypes.setDate(adapter, "dateField", testTimestamp);
        assertEquals(testDate, adapter.getDate("dateField"));
    }

    @Test
    public void setTimestamp() throws Exception {
        DataTypes.setTimestamp(adapter, "foo", null);
        assertNull(adapter.get("foo"));

        adapter.clear();

        DataTypes.setTimestamp(adapter, "timestampField", testTimestamp);
        assertEquals(testTimestamp, adapter.getTimestamp("timestampField"));

        adapter.clear();

        DataTypes.setTimestamp(adapter, "timestampField", testTimestamp.toString());
        assertEquals(testTimestamp, adapter.getTimestamp("timestampField"));

        adapter.clear();

        DataTypes.setTimestamp(adapter, "timestampField", testLocalDate);
        assertEquals(testTimestamp, adapter.getTimestamp("timestampField"));

        adapter.clear();
        System.out.println(testTimestamp.toString());
        DataTypes.setTimestamp(adapter, "timestampField", JsonNodeFactory.instance.textNode("2020-05-07 15:20:10"));
        assertEquals(Timestamp.valueOf("2020-05-07 15:20:10.0"), adapter.getTimestamp("timestampField"));

        adapter.clear();

        DataTypes.setTimestamp(adapter, "timestampField", testTimestamp);
        assertEquals(testTimestamp, adapter.getTimestamp("timestampField"));
    }

    @Test
    public void valueOfClass() {
        assertEquals(DataTypes.string, DataTypes.valueOf(String.class));
        assertEquals(DataTypes.string, DataTypes.valueOf(String[].class));

        assertEquals(DataTypes.int32, DataTypes.valueOf(int.class));
        assertEquals(DataTypes.int32, DataTypes.valueOf(int[].class));
        assertEquals(DataTypes.int32, DataTypes.valueOf(Integer.class));
        assertEquals(DataTypes.int32, DataTypes.valueOf(Integer[].class));
        assertEquals(DataTypes.int32, DataTypes.valueOf(short.class));
        assertEquals(DataTypes.int32, DataTypes.valueOf(short[].class));
        assertEquals(DataTypes.int32, DataTypes.valueOf(Short.class));
        assertEquals(DataTypes.int32, DataTypes.valueOf(Short[].class));
        assertEquals(DataTypes.int32, DataTypes.valueOf(byte.class));
        assertEquals(DataTypes.int32, DataTypes.valueOf(byte[].class));
        assertEquals(DataTypes.int32, DataTypes.valueOf(Byte.class));
        assertEquals(DataTypes.int32, DataTypes.valueOf(Byte[].class));

        assertEquals(DataTypes.int64, DataTypes.valueOf(long.class));
        assertEquals(DataTypes.int64, DataTypes.valueOf(long[].class));
        assertEquals(DataTypes.int64, DataTypes.valueOf(Long.class));
        assertEquals(DataTypes.int64, DataTypes.valueOf(Long[].class));
        assertEquals(DataTypes.int64, DataTypes.valueOf(BigInteger.class));
        assertEquals(DataTypes.int64, DataTypes.valueOf(BigInteger[].class));

        assertEquals(DataTypes.float64, DataTypes.valueOf(double.class));
        assertEquals(DataTypes.float64, DataTypes.valueOf(double[].class));
        assertEquals(DataTypes.float64, DataTypes.valueOf(Double.class));
        assertEquals(DataTypes.float64, DataTypes.valueOf(Double[].class));
        assertEquals(DataTypes.float64, DataTypes.valueOf(float.class));
        assertEquals(DataTypes.float64, DataTypes.valueOf(float[].class));
        assertEquals(DataTypes.float64, DataTypes.valueOf(Float.class));
        assertEquals(DataTypes.float64, DataTypes.valueOf(Float[].class));

        assertEquals(DataTypes.bool, DataTypes.valueOf(boolean.class));
        assertEquals(DataTypes.bool, DataTypes.valueOf(boolean[].class));
        assertEquals(DataTypes.bool, DataTypes.valueOf(Boolean.class));
        assertEquals(DataTypes.bool, DataTypes.valueOf(Boolean[].class));

        assertEquals(DataTypes.date, DataTypes.valueOf(Date.class));
        assertEquals(DataTypes.date, DataTypes.valueOf(Date[].class));

        assertEquals(DataTypes.localDate, DataTypes.valueOf(LocalDate.class));
        assertEquals(DataTypes.localDate, DataTypes.valueOf(LocalDate[].class));

        assertEquals(DataTypes.timestamp, DataTypes.valueOf(Timestamp.class));
        assertEquals(DataTypes.timestamp, DataTypes.valueOf(Timestamp[].class));
    }

    @Test
    public void jsonConverterForMap() throws Exception {
        Map<String, Integer> map = new HashMap<>();
        map.put("foo", 222);
        map.put("bar", 777);

        assertEquals("{\"bar\":777,\"foo\":222}", DataTypes.jsonConverter(map).toString());

    }

    @Test
    public void jsonConverterForList() throws Exception {
        List<String> list = new ArrayList<>();
        list.add("foo");
        list.add("bar");

        assertEquals("[\"foo\",\"bar\"]", DataTypes.jsonConverter(list).toString());
    }
}