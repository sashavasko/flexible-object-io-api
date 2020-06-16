package org.sv.flexobject.adapter;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.sv.flexobject.stream.Source;

import java.lang.reflect.InvocationTargetException;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Map;

import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.class)
public class GenericInAdapterTest {

    @Mock
    Source mockSource;

    @Mock
    Source mockSource2;

    @Mock
    Map mockRecord;

    @Mock
    Map mockRecord2;


    static class TestAdapter extends GenericInAdapter<Map>{
        public TestAdapter(Source<Map> source) {
            super(source);
        }

        @Override
        public String getString(String fieldName) throws Exception {
            return (String) getCurrent().get(fieldName);
        }

        @Override
        public JsonNode getJson(String fieldName) throws Exception {
            return (JsonNode) getCurrent().get(fieldName);
        }

        @Override
        public Integer getInt(String fieldName) throws Exception {
            return (Integer) getCurrent().get(fieldName);
        }

        @Override
        public Boolean getBoolean(String fieldName) throws Exception {
            return (Boolean) getCurrent().get(fieldName);
        }

        @Override
        public Long getLong(String fieldName) throws Exception {
            return (Long) getCurrent().get(fieldName);
        }

        @Override
        public Date getDate(String fieldName) throws Exception {
            return (Date) getCurrent().get(fieldName);
        }

        @Override
        public Timestamp getTimestamp(String fieldName) throws Exception {
            return (Timestamp) getCurrent().get(fieldName);
        }
    }

    GenericInAdapter adapter;

    @Before
    public void setUp() throws Exception {
        adapter = GenericInAdapter.build(TestAdapter.class, mockSource);
        Mockito.when(mockSource.get()).thenReturn(mockRecord,mockRecord2);
        Mockito.when(mockSource.isEOF()).thenReturn(false, false, true);

    }

    @Test
    public void build() throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        assertSame(mockSource, adapter.getSource());
    }

    @Test
    public void setParam() {
        adapter.setParam(GenericInAdapter.PARAMS.source.name(), mockSource2);

        assertSame(mockSource2, adapter.getSource());
    }

    @Test
    public void nextAndGetCurrent() throws Exception {
        assertNull(adapter.getCurrent());

        assertTrue(adapter.next());
        assertSame(mockRecord, adapter.getCurrent());

        assertTrue(adapter.next());
        assertSame(mockRecord2, adapter.getCurrent());

        assertFalse(adapter.next());
    }

    @Test
    public void ack() {
        adapter.ack();

        Mockito.verify(mockSource).ack();
    }

    @Test
    public void setEOF() {
        adapter.setEOF();

        Mockito.verify(mockSource).setEOF();
    }

    @Test
    public void close() throws Exception {
        adapter.close();

        Mockito.verify(mockSource).close();
    }
}