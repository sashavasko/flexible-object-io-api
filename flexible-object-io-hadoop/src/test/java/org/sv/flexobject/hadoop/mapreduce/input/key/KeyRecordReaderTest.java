package org.sv.flexobject.hadoop.mapreduce.input.key;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.sv.flexobject.InAdapter;
import org.sv.flexobject.hadoop.mapreduce.input.AdapterRecordReader;
import org.sv.flexobject.hadoop.mapreduce.input.DaoRecordReaderConf;
import org.sv.flexobject.stream.report.ProgressReporter;
import org.sv.flexobject.util.InstanceFactory;

import java.io.IOException;
import java.sql.SQLException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class KeyRecordReaderTest {

    @Mock
    KeyInputDao dao;

    @Mock
    KeyInputSplit split;

    @Mock
    InAdapter input;

    @Mock
    Writable key;

    @Mock
    LongWritable longKey;

    @Mock
    LongWritable longValue;

    @Mock
    Text textValue;

    @Mock
    TaskAttemptContext context;

    @Mock
    ProgressReporter progressReporter;

    @Mock
    AdapterRecordReader.LongField longKeyField;

    @Mock
    AdapterRecordReader.LongField longValueField;

    @Mock
    AdapterRecordReader.TextField textValueField;

    IOException ioException;

    SQLException sqlException;

    DaoRecordReaderConf conf;

    KeyRecordReader reader;

    @Before
    public void setUp() throws Exception {
        reader = Mockito.mock(KeyRecordReader.class, Mockito.CALLS_REAL_METHODS);
        conf = new DaoRecordReaderConf();
        doReturn(dao).when(reader).getDao();
        doReturn(split).when(reader).getSplit();
        doReturn(conf).when(reader).getConf();
        doReturn(progressReporter).when(reader).getProgressReporter();

        ioException = new IOException("io is bad");
        sqlException = new SQLException("sql is no good");
    }

    @After
    public void tearDown() throws Exception {
        InstanceFactory.reset();
    }

    @Test
    public void createAdapter() throws Exception {
        doReturn(key).when(split).getKey();
        doReturn(input).when(dao).start(key);

        assertSame(input, reader.createAdapter(split, context));
    }

    @Test
    public void createAdapterWithIOException() throws Exception {
        doReturn(key).when(split).getKey();
        doThrow(ioException).when(dao).start(key);

        try {
            reader.createAdapter(split, context);
            throw new RuntimeException("Should re-throw IOException");
        }catch (IOException actualException) {
            assertSame(ioException, actualException);
            assertEquals("io is bad", actualException.getMessage());
        }
    }

    @Test
    public void createAdapterWithNonIOException() throws Exception {
        doReturn(key).when(split).getKey();
        doThrow(sqlException).when(dao).start(key);

        try {
            reader.createAdapter(split, context);
            throw new RuntimeException("Should wrap exception into IOException");
        }catch (IOException actualException) {
            assertSame(sqlException, actualException.getCause());
            assertEquals("Failed to query DAO by the key in Configuration org.sv.flexobject.hadoop.mapreduce.input.DaoRecordReaderConf{{\"keyFieldName\":\"CURRENT_KEY\",\"valueFieldName\":\"CURRENT_VALUE\",\"maxRetries\":3}}", actualException.getMessage());
        }
    }

    @Test
    public void nextKeyValue() throws Exception {
        doReturn(input).when(reader).getInput();
        doReturn(true).doReturn(true).doReturn(false).when(input).next();

        assertTrue(reader.nextKeyValue());
        assertTrue(reader.nextKeyValue());
        assertFalse(reader.nextKeyValue());

        verify(progressReporter,times(2)).increment();
    }

    @Test
    public void nextKeyValueWithException() throws Exception {
        doReturn(input).when(reader).getInput();
        doReturn(true).doThrow(ioException).when(input).next();

        assertTrue(reader.nextKeyValue());
        assertFalse(reader.nextKeyValue());

        verify(progressReporter,times(1)).increment();
    }

    @Test
    public void longLongDong() throws Exception {
        reader = Mockito.mock(KeyRecordReader.LongLong.class, Mockito.CALLS_REAL_METHODS);
        doReturn(longKeyField).doReturn(longValueField).when(reader).longField();
        doReturn("keyName").when(reader).getKeyFieldName();
        doReturn("valueName").when(reader).getValueFieldName();
        doReturn(longKey).when(longKeyField).convert("keyName");
        doReturn(longValue).when(longValueField).convert("valueName");

        assertSame(longKey, reader.getCurrentKey());
        assertSame(longValue, reader.getCurrentValue());
    }

    @Test
    public void longTextSong() throws Exception {
        reader = Mockito.mock(KeyRecordReader.LongText.class, Mockito.CALLS_REAL_METHODS);
        doReturn(longKeyField).when(reader).longField();
        doReturn(textValueField).when(reader).textField();
        doReturn("keyName").when(reader).getKeyFieldName();
        doReturn("valueName").when(reader).getValueFieldName();
        doReturn(longKey).when(longKeyField).convert("keyName");
        doReturn(textValue).when(textValueField).convert("valueName");

        assertSame(longKey, reader.getCurrentKey());
        assertSame(textValue, reader.getCurrentValue());
    }
}