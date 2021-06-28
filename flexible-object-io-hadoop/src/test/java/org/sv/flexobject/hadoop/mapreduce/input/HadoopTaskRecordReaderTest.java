package org.sv.flexobject.hadoop.mapreduce.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.sv.flexobject.hadoop.HadoopTask;
import org.sv.flexobject.stream.report.ProgressReporter;
import org.sv.flexobject.stream.report.SimpleProgressReporter;
import org.sv.flexobject.util.InstanceFactory;

import java.io.IOException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class HadoopTaskRecordReaderTest {
    @Mock
    Configuration mockConf;

    @Mock
    TaskAttemptContext mockContext;

    @Mock
    InputSplit mockSplit;

    @Mock
    Object mockKey;

    @Mock
    Object mockValue;

    @Mock
    ProgressReporter mockProgressReporter;

    public static class TestException extends RuntimeException{
        @Override
        public String getMessage() {
            return "TestException";
        }
    }

    HadoopTaskRecordReader reader;

    @Before
    public void setUp() throws Exception {
        InstanceFactory.set(SimpleProgressReporter.class, mockProgressReporter);

        reader = Mockito.mock(HadoopTaskRecordReader.class, Mockito.CALLS_REAL_METHODS);
        when(mockContext.getConfiguration()).thenReturn(mockConf);
    }

    @After
    public void tearDown() throws Exception {
        InstanceFactory.reset();
    }

    @Test()
    public void initialize() throws IOException, InterruptedException {

        doNothing().when(reader).setupInput(mockSplit, mockContext);

        reader.initialize(mockSplit, mockContext);

        assertSame(mockConf, HadoopTask.getInstance().getConf());
        assertSame(mockSplit, reader.getSplit());
        verify(reader).setupInput(mockSplit, mockContext);
    }

    @Test()
    public void initializeWithException() throws IOException, InterruptedException {

        doThrow(Exception.class).when(mockContext).getConfiguration();

        try {
            reader.initialize(mockSplit, mockContext);
        }catch(RuntimeException e){
            assertEquals("Failed to initialize HadoopTask", e.getMessage());
            assertNotNull(e.getCause());
        }
    }

    @Test()
    public void initializeWithRuntimeException() throws IOException, InterruptedException {

        doThrow(TestException.class).when(mockContext).getConfiguration();

        try {
            reader.initialize(mockSplit, mockContext);
        }catch(RuntimeException e){
            assertNull(e.getCause());
            assertEquals(TestException.class, e.getClass());
        }
    }

    @Test
    public void getCurrentKey() throws Exception {
        doReturn(mockKey).when(reader).convertCurrentKey();
        assertSame(mockKey, reader.getCurrentKey());
    }

    @Test
    public void getCurrentKeyException() throws Exception {
        doThrow(TestException.class).when(reader).convertCurrentKey();
        assertNull(reader.getCurrentKey());
    }

    @Test
    public void getCurrentValue() throws Exception {
        doReturn(mockValue).when(reader).convertCurrentValue();
        assertSame(mockValue, reader.getCurrentValue());
    }

    @Test
    public void getCurrentValueException() throws Exception {
        doThrow(TestException.class).when(reader).convertCurrentValue();
        assertNull(reader.getCurrentValue());
    }

    @Test
    public void getProgress() throws IOException, InterruptedException {
        reader.getProgress();

        verify(mockProgressReporter).getProgress();
    }

    @Test
    public void getProgressReporter() {
        InstanceFactory.reset();
        reader.setProgressReporter(mockProgressReporter);

        assertSame(mockProgressReporter, reader.getProgressReporter());
    }
}