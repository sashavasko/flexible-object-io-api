package org.sv.flexobject.hadoop.mapreduce.input;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.sv.flexobject.InAdapter;
import org.sv.flexobject.hadoop.mapreduce.util.MRDao;
import org.sv.flexobject.properties.Namespace;
import org.sv.flexobject.util.InstanceFactory;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
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

    @BeforeEach
    public void setUp() throws Exception {
        conf = new DaoRecordReaderConf(Namespace.forPath(".", "test"));
        rawConf = new Configuration(false);
        rawConf.set("test.record.reader.key.field.name", "keyField");
        rawConf.set("test.record.reader.value.field.name", "valueField");
        rawConf.setInt("test.record.reader.max.retries", 777);
        rawConf.set("test.record.reader.dao.class", String.class.getName());

        doReturn(rawConf).when(mockContext).getConfiguration();
        InstanceFactory.set(String.class, mockDao);
        InstanceFactory.set(DaoRecordReaderConf.class, conf);

        reader = Mockito.mock(DaoRecordReader.class, Mockito.CALLS_REAL_METHODS);
    }

    @AfterEach
    public void tearDown() throws Exception {
        InstanceFactory.reset();
    }

    @Test
    public void setupInputNoDao() {
        rawConf.unset("test.record.reader.dao.class");

        assertThrows(RuntimeException.class, ()-> {reader.setupInput(mockSplit, mockContext);});
    }

    @Test
    public void setupInputAdapterFail() throws IOException {

        IOException testException = new IOException("tits");

        rawConf.setInt("test.record.reader.max.retries", 2);
        doThrow(testException).when(reader).createAdapter(mockSplit, mockContext);


        assertThrows(RuntimeException.class, ()-> {reader.setupInput(mockSplit, mockContext);});
    }

    @Test
    public void setupInput() throws IOException {
        doReturn(mockAdapter).when(reader).createAdapter(mockSplit, mockContext);
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
        doReturn(mockAdapter).when(reader).createAdapter(mockSplit, mockContext);
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