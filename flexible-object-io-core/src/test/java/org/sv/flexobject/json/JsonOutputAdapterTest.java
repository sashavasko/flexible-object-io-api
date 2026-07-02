package org.sv.flexobject.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.sv.flexobject.SaveException;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.stream.sinks.SingleValueSink;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.Temporal;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
public class JsonOutputAdapterTest {

    ObjectReader objectReader;

    SingleValueSink<ObjectNode> sink = new SingleValueSink<>();
    JsonOutputAdapter adapter;

    @BeforeEach
    public void setUp() throws Exception {
        objectReader = new ObjectMapper().reader();
        MapperFactory.setObjectReader(objectReader);
        adapter = new JsonOutputAdapter(sink);
    }

    @AfterEach
    public void tearDown() {
        adapter = null;
    }

    @Test(expected = SaveException.class)
    public void setStringNull() throws Exception {
        adapter.setString("field", null);
        adapter.save();
    }

    @Test
    public void defaultConstructor() {
        adapter = new JsonOutputAdapter();
    }

    @Test
    public void setString() throws Exception {
        adapter.setString("field", "yes");
        adapter.save();

        assertEquals("yes", sink.get().get("field").asText());
    }

    @Test(expected = SaveException.class)
    public void setJsonNull() throws Exception {
        adapter.setJson("field", null);
        adapter.save();
    }

    @Test
    public void setJson() throws Exception {
        String value = "{'a':1,'b':7}".replace('\'', '"');
        JsonNode valueJson = MapperFactory.getObjectReader().readTree(value);
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


    @Test(expected = SaveException.class)
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

    @Test(expected = SaveException.class)
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

    @Test(expected = SaveException.class)
    public void setLongNotNull() throws Exception {
        Long value = null;
        adapter.setLong("field", value);
        adapter.save();
    }

    @Test
    public void setDate() throws Exception {
        long millis = 1544116803000l;
        /*
            GMT: Thursday, December 6, 2018 5:20:03 PM
            Your time zone: Thursday, December 6, 2018 11:20:03 AM GMT-06:00
         */

        adapter = new JsonOutputAdapter(sink);
        Date date = new Date(millis);
        adapter.setDate("date", date);
        adapter.save();
        ObjectNode json = sink.get();
        String actual = json.toString();
        assertEquals("{\"date\":\"2018-12-06T17:20:03Z\"}", actual);
        String actualDateString = json.get("date").asText();
        LocalDateTime expectedDate = LocalDateTime.of(2018, 12, 6, 11, 20, 3);
        LocalDateTime actualDate = LocalDateTime.parse(actualDateString, DateTimeFormatter.ISO_INSTANT.withZone(ZoneId.systemDefault()));
        System.out.println(actualDate);
        assertEquals(millis, DataTypes.dateConverter(actualDate).getTime());
        assertEquals(millis, DataTypes.dateConverter(actualDateString).getTime());
        assertEquals(expectedDate, actualDate);

    }

    @Test
    public void setDateLegacy() throws Exception {
        long millis = 1544116803000l;
        /*
            GMT: Thursday, December 6, 2018 5:20:03 PM
            Your time zone: Thursday, December 6, 2018 11:20:03 AM GMT-06:00
         */

        adapter = new JsonOutputAdapter(sink, JsonOutputAdapter::dateToJsonNodeUsingLegacyFormat);
        Date date = new Date(millis);
        adapter.setDate("date", date);
        adapter.save();
        ObjectNode json = sink.get();
        String actual = json.toString();
        assertEquals("{\"date\":\"Dec 6, 2018 05:20:03 PM\"}", actual);
        String actualDateString = json.get("date").asText();
        LocalDateTime expectedDate = LocalDateTime.of(2018, 12, 6, 11, 20, 3);
        Temporal actualDate = DataTypes.parseDateString(actualDateString);
        System.out.println(actualDate);
        assertEquals(millis, DataTypes.dateConverter(actualDate).getTime());
        assertEquals(millis, DataTypes.dateConverter(actualDateString).getTime());
        assertEquals(expectedDate, actualDate);

    }

    @Test(expected = SaveException.class)
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

    @Test(expected = SaveException.class)
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

    @Test
    public void produce() throws Exception {
        ObjectNode jsonOut = JsonOutputAdapter.produce(adapter->{
            adapter.setString("a", "foo");
            adapter.setInt("b", 1234567);
            adapter.save();
        });

        assertEquals(MapperFactory.getObjectReader().readTree("{'a':'foo','b':1234567}".replace('\'', '"')), jsonOut);
    }
}