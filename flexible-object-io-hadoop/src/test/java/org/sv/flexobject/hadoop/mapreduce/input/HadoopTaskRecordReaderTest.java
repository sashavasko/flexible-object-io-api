package org.sv.flexobject.hadoop.mapreduce.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.sv.flexobject.hadoop.HadoopTask;
import org.sv.flexobject.stream.report.ProgressReporter;
import org.sv.flexobject.stream.report.SimpleProgressReporter;
import org.sv.flexobject.util.InstanceFactory;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
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

    @BeforeEach
    public void setUp() throws Exception {
        HadoopTask.clearConfiguration();
        InstanceFactory.set(SimpleProgressReporter.class, mockProgressReporter);

        reader = Mockito.mock(HadoopTaskRecordReader.class, Mockito.CALLS_REAL_METHODS);
        when(mockContext.getConfiguration()).thenReturn(mockConf);
    }

    @AfterEach
    public void tearDown() throws Exception {
        InstanceFactory.reset();
        HadoopTask.clearConfiguration();
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