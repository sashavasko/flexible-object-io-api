package org.sv.flexobject.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.sv.flexobject.copy.CopyAdapter;
import org.sv.flexobject.stream.sources.QueueSource;

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(MockitoJUnitRunner.class)
public class JsonInputAdapterTest {

    JsonNode json = null;
    ObjectReader objectReader = new ObjectMapper().reader();

    @Mock
    ObjectNode mockJson;

    @Mock
    CopyAdapter copyAdapter = new CopyAdapter();

    Queue<JsonNode> values = new ArrayDeque<>(2);
    QueueSource<JsonNode> source;

    JsonInputAdapter adapter;

    @Before
    public void setUp() throws Exception {
        MapperFactory.setObjectReader(objectReader);
        source = new QueueSource<>(values);
        adapter = new JsonInputAdapter(source);
    }

    @After
    public void tearDown() throws Exception {
        values.clear();
    }

    @Test
    public void setJson() throws InterruptedException, TimeoutException {
        values.add(mockJson);
        assertNull(adapter.getCurrent());
    }

    @Test
    public void getString() throws Exception {
        json = objectReader.readTree("{'a':'valueString'}".replace('\'', '"'));
        values.add(json);
        adapter.next();
        assertEquals("valueString", adapter.getString("a"));
        assertNull(adapter.getString("badField"));
    }

    @Test
    public void getJson() throws Exception {
        json = objectReader.readTree("{'a':{'subfield':'valueString'}}".replace('\'', '"'));
        values.add(json);
        adapter.next();
        assertEquals(objectReader.readTree("{\"subfield\":\"valueString\"}"), adapter.getJson("a"));
    }

    @Test
    public void getStringFromObjectNode() throws Exception {
        json = objectReader.readTree("{'a':{'subfield':'valueString'}}".replace('\'', '"'));
        values.add(json);
        adapter.next();
        assertEquals("{\"subfield\":\"valueString\"}", adapter.getString("a"));
    }

    @Test
    public void getInt() throws Exception {
        json = objectReader.readTree("{'a':12345678}".replace('\'', '"'));
        values.add(json);
        assertTrue(adapter.next());
        assertEquals(12345678, (int) adapter.getInt("a"));
        assertNull(adapter.getInt("badField"));
    }

    @Test
    public void getBoolean() throws Exception {
        json = objectReader.readTree("{'a':true, 'b':'false'}".replace('\'', '"'));
        values.add(json);
        adapter.next();
        assertTrue(adapter.getBoolean("a"));
        assertFalse(adapter.getBoolean("b"));
        assertNull(adapter.getBoolean("badField"));
    }

    @Test
    public void getLong() throws Exception {
        json = objectReader.readTree("{'a':12345678000}".replace('\'', '"'));
        values.add(json);
        adapter.next();
        assertEquals(12345678000l, (long) adapter.getLong("a"));
        assertNull(adapter.getLong("badField"));
    }

    @Test
    public void getDate() throws Exception {
        json = objectReader.readTree("{\"date\":\"Dec 06, 2018 05:20:03 PM\"}");
        values.add(json);
        adapter.next();
        Date expectedDate = new Date(1544116803000l);
        assertEquals(expectedDate.getTime(), adapter.getDate("date").getTime());
        assertNull(adapter.getDate("badField"));
    }

    @Test
    public void getTimestamp() throws Exception {
        json = objectReader.readTree("{\"timestamp\":1544116803000}");
        values.add(json);
        adapter.next();
        Timestamp expectedDate = new Timestamp(1544116803000l);
        assertEquals(expectedDate.getTime(), adapter.getTimestamp("timestamp").getTime());
        assertNull(adapter.getTimestamp("badField"));
    }

    @Test(expected = NullPointerException.class)
    public void nextHandlesNull() throws Exception {
        values.add(null);
        assertFalse(adapter.next());
    }

    @Test
    public void nextKeepsOnMoving() throws Exception {
        ObjectNode n1 = JsonNodeFactory.instance.objectNode();
        n1.put("a", "string1");
        values.add(n1);
        ObjectNode n2 = JsonNodeFactory.instance.objectNode();
        n2.put("a", "string2");
        values.add(n2);
        assertTrue(adapter.next());
        assertEquals("string1", adapter.getString("a"));
        assertTrue(adapter.next());
        assertEquals("string2", adapter.getString("a"));
        assertFalse(adapter.next());
    }

    @Test
    public void consume() throws Exception {
        json = objectReader.readTree("{'a':'foo', 'b':12345}}".replace('\'', '"'));
        JsonInputAdapter.consume(json, adapter->{
           assertEquals("foo", adapter.getString("a"));
           assertEquals(12345, (int)adapter.getInt("b"));
        });
    }

    @Test
    public void copyRecord() throws Exception {
        json = objectReader.readTree("{'bool':true,'number':1234567,'string':'data','object':{'subfield':'blah'}}".replace('\'', '"'));
        values.add(json);
        adapter.next();

        adapter.copyRecord(copyAdapter);

        verify(copyAdapter).put("bool", true);
        verify(copyAdapter).put("number", 1234567l);
        verify(copyAdapter).put("string", "data");
        verify(copyAdapter).put("object", objectReader.readTree("{'subfield':'blah'}".replace('\'', '"')));
        verifyNoMoreInteractions(copyAdapter);
    }
}