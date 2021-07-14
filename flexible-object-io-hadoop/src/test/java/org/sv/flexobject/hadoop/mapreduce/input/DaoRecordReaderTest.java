package org.sv.flexobject.hadoop.mapreduce.input;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.sv.flexobject.InAdapter;
import org.sv.flexobject.hadoop.mapreduce.util.MRDao;
import org.sv.flexobject.util.InstanceFactory;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class DaoRecordReaderTest {

    @Mock
    InputSplit mockSplit;

    @Mock
    TaskAttemptContext mockContext;

    @Mock
    InAdapter mockAdapter;

    @Mock(extraInterfaces = {Configurable.class})
    MRDao mockDao;

    @Mock Exception mockException;

    @Mock
    Logger mockLogger;

    Configuration rawConf;
    DaoRecordReaderConf conf;

    DaoRecordReader reader;

    @Before
    public void setUp() throws Exception {
        conf = new DaoRecordReaderConf("test");
        rawConf = new Configuration(false);
        rawConf.set("test.record.reader.key.field.name", "keyField");
        rawConf.set("test.record.reader.value.field.name", "valueField");
        rawConf.setInt("test.record.reader.max.retries", 777);
        rawConf.set("test.record.reader.dao.class", String.class.getName());

        doReturn(rawConf).when(mockContext).getConfiguration();
        InstanceFactory.set(String.class, mockDao);
        InstanceFactory.set(DaoRecordReaderConf.class, conf);

        reader = Mockito.mock(DaoRecordReader.class, Mockito.CALLS_REAL_METHODS);
        doReturn(mockAdapter).when(reader).createAdapter(mockSplit, mockContext);
    }

    @After
    public void tearDown() throws Exception {
        InstanceFactory.reset();
    }

    @Test(expected = RuntimeException.class)
    public void setupInputNoDao() {
        rawConf.unset("test.record.reader.dao.class");
        reader.setupInput(mockSplit, mockContext);
    }

    @Test(expected = RuntimeException.class)
    public void setupInputAdapterFail() throws IOException {

        IOException testException = new IOException("tits");

        rawConf.setInt("test.record.reader.max.retries", 2);
        doThrow(testException).when(reader).createAdapter(mockSplit, mockContext);

        reader.setupInput(mockSplit, mockContext);
    }

    @Test
    public void setupInput() {
        reader.setupInput(mockSplit, mockContext);

        assertSame(conf, reader.getConf());
        assertEquals("keyField", reader.getKeyFieldName());
        assertEquals("valueField", reader.getValueFieldName());
        assertSame(mockDao, reader.getDao());
        verify((Configurable)mockDao).setConf(rawConf);
        assertSame(mockAdapter, reader.getInput());
    }

    @Test
    public void close() throws Exception {
        reader.setupInput(mockSplit, mockContext);

        reader.close();

        verify(mockAdapter).close();
        verify(mockDao).close();
    }

    @Test
    public void closeWithException() throws Exception {
        reader.setupInput(mockSplit, mockContext);
        reader.logger = mockLogger;
        doThrow(mockException).when(mockDao).close();

        try {
            reader.close();
            throw new RuntimeException("Should have thrown");
        }catch(Exception e){
            verify(mockLogger).error("Failed to close DAO", mockException);;
        }
    }
}