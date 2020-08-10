package org.sv.flexobject.adapter;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.sv.flexobject.stream.Sink;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class GenericOutAdapterTest {

    @Mock
    Sink mockSink;

    @Mock
    Sink mockSink2;

    @Mock
    Map mockRecord;

    @Mock
    Map mockRecord2;

    public static class TestAdapter extends GenericOutAdapter<Map>{

        static Iterator<Map> recordsIterator;

        static void setRecords(List<Map> records){
            recordsIterator = records.iterator();
        }

        public TestAdapter() {
        }

        public TestAdapter(Sink sink) {
            super(sink);
        }

        @Override
        public Map createRecord() {
            return recordsIterator.next();
        }

        @Override
        public void setString(String paramName, String value) throws Exception {

        }

        @Override
        public void setJson(String paramName, JsonNode value) throws Exception {

        }

        @Override
        public void setInt(String paramName, Integer value) throws Exception {

        }

        @Override
        public void setBoolean(String paramName, Boolean value) throws Exception {

        }

        @Override
        public void setLong(String paramName, Long value) throws Exception {

        }

        @Override
        public void setDouble(String paramName, Double value) throws Exception {

        }

        @Override
        public void setDate(String paramName, Date value) throws Exception {

        }

        @Override
        public void setTimestamp(String paramName, Timestamp value) throws Exception {

        }
    }

    GenericOutAdapter adapter;

    @Before
    public void setUp() throws Exception {
        TestAdapter.setRecords(Arrays.asList(mockRecord, mockRecord2));
        adapter = GenericOutAdapter.build(TestAdapter.class, mockSink);
    }

    @Test
    public void setParam() {
        assertSame(mockSink, adapter.getSink());

        adapter.setParam(GenericOutAdapter.PARAMS.sink.name(), mockSink2);

        assertSame(mockSink2, adapter.getSink());
    }

    @Test
    public void getCurrent() throws Exception {
        assertSame(mockRecord, adapter.getCurrent());

        adapter.save();

        assertSame(mockRecord2, adapter.getCurrent());
    }

    @Test
    public void convertRecordForSink() {
        assertSame(mockRecord2, adapter.convertRecordForSink(mockRecord2));
    }

    @Test(expected = IllegalArgumentException.class)
    public void saveWithoutGetCurrent() throws Exception {
        adapter.save();
    }

    @Test
    public void shouldSave() throws Exception {
        assertFalse(adapter.shouldSave());

        adapter.getCurrent();

        assertTrue(adapter.shouldSave());

        adapter.save();

        assertFalse(adapter.shouldSave());

    }

    @Test
    public void hasOutput() {
        assertFalse(adapter.hasOutput());

        when(mockSink.hasOutput()).thenReturn(true);

        assertTrue(adapter.hasOutput());

        verify(mockSink, times(2)).hasOutput();
    }

}