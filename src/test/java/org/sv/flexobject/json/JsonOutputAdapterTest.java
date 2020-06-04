package org.sv.flexobject.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.sv.flexobject.stream.sinks.SingleValueSink;

import java.sql.Date;
import java.sql.Timestamp;

import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.class)
public class JsonOutputAdapterTest {

    ObjectReader objectReader;

    SingleValueSink<ObjectNode> sink = new SingleValueSink<>();
    JsonOutputAdapter adapter;

    @Before
    public void setUp() throws Exception {
        objectReader = new ObjectMapper().reader();
        MapperFactory.setObjectReader(objectReader);
        adapter = new JsonOutputAdapter(sink);
    }

    @After
    public void tearDown() {
        adapter = null;
    }

    @Test(expected = IllegalArgumentException.class)
    public void setStringNull() throws Exception {
        adapter.setString("field", null);
        adapter.save();
    }

    @Test
    public void setString() throws Exception {
        adapter.setString("field", "yes");
        adapter.save();

        assertEquals("yes", sink.get().get("field").asText());
    }

    @Test(expected = IllegalArgumentException.class)
    public void setJsonNull() throws Exception {
        adapter.setJson("field", null);
        adapter.save();
    }

    @Test
    public void setJson() throws Exception {
        String value = "{'a':1,'b':7}".replace('\'', '"');
        JsonNode valueJson = objectReader.readTree(value);
        adapter.setJson("field", valueJson);
        adapter.save();
        assertEquals(valueJson, sink.get().get("field"));
    }

    @Test
    public void setBoolean() throws Exception {
        Boolean value = true;
        adapter.setBoolean("field", value);
        adapter.save();
        assertEquals(value, sink.get().get("field").asBoolean());
    }


    @Test(expected = IllegalArgumentException.class)
    public void setBooleanNotNull() throws Exception {
        Boolean value = null;
        adapter.setBoolean("field", value);
        adapter.save();
    }

    @Test
    public void setInt() throws Exception {
        Integer value = 1;
        adapter.setInt("field", value);
        adapter.save();
        assertEquals((int) value, sink.get().get("field").asInt());
        assertFalse(adapter.hasOutput());
    }

    @Test(expected = IllegalArgumentException.class)
    public void setIntNotNull() throws Exception {
        Integer value = null;
        adapter.setInt("field", value);
        adapter.save();
    }

    @Test
    public void setLong() throws Exception {
        Long value = 1000000000000l;
        adapter.setLong("field", value);
        adapter.save();
        assertEquals((long) value, sink.get().get("field").asLong());
    }

    @Test(expected = IllegalArgumentException.class)
    public void setLongNotNull() throws Exception {
        Long value = null;
        adapter.setLong("field", value);
        adapter.save();
    }

    @Test
    public void setDate() throws Exception {
        adapter = new JsonOutputAdapter(sink);
        Date date = new Date(1544116803000l);
        adapter.setDate("date", date);
        adapter.save();
        assertEquals("{\"date\":\"Dec 06, 2018 05:20:03 PM\"}", sink.get().toString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void setDateNotNull() throws Exception {
        Date date = null;
        adapter.setDate("date", date);
        adapter.save();
    }

    @Test
    public void setTimestamp() throws Exception {
        adapter = new JsonOutputAdapter(sink);
        Timestamp date = new Timestamp(1544116803000l);
        adapter.setTimestamp("ts", date);
        adapter.save();

        assertEquals("{\"ts\":1544116803000}", sink.get().toString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void setTimestampNotNull() throws Exception {
        Timestamp ts = null;
        adapter.setTimestamp("ts", ts);
        adapter.save();
    }

    @Test
    public void save() throws Exception {
        adapter.setLong("field", 1l);
        adapter.save();
        assertTrue(adapter.hasOutput());
    }
}