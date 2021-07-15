package org.sv.flexobject.hadoop.mapreduce.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.sv.flexobject.util.InstanceFactory;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ConfiguredInputFormatTest {

    @Mock
    InputConf conf;

    @Mock
    JobContext mockContext;

    @Mock
    TaskAttemptContext mockTAContext;

    @Mock
    InputSplit mockSplit;

    @Mock
    Configuration rawConf;

    @Mock
    Splitter mockSplitter;

    @Mock
    List<InputSplit> mockSplits;

    @Mock
    RecordReader mockReader;

    ConfiguredInputFormat format;

    @Before
    public void setUp() throws Exception {
        format = Mockito.mock(ConfiguredInputFormat.class, Mockito.CALLS_REAL_METHODS);
        doReturn(conf).when(format).makeInputConf();
        doReturn(conf).when(conf).from(rawConf);
        doReturn(rawConf).when(mockContext).getConfiguration();
        doReturn(rawConf).when(mockTAContext).getConfiguration();
        doReturn(mockSplitter).when(conf).getSplitter();
        doReturn(mockSplits).when(mockSplitter).split(rawConf);
        doReturn(mockReader).when(conf).getReader();
    }

    @After
    public void tearDown() throws Exception {
        InstanceFactory.reset();
    }

    @Test
    public void getSplits() throws IOException, InterruptedException {
        assertSame(mockSplits, format.getSplits(mockContext));
        verify(format).makeInputConf();
        verify(conf).from(rawConf);
        verify(mockSplitter).split(rawConf);
    }

    @Test(expected = RuntimeException.class)
    public void getSplitsWithException() throws IOException, InterruptedException {
        IOException mockException = new IOException("foo");
        doThrow(mockException).when(mockSplitter).split(rawConf);
        doReturn(new RuntimeException(mockException)).when(conf).runtimeException(format.logger, "Failed to instantiate splitter", mockException);

        format.getSplits(mockContext);

        verify(conf).runtimeException(format.logger, "Failed to instantiate splitter", mockException);
    }

    @Test
    public void createRecordReader() throws IOException, InterruptedException {
        assertSame(mockReader, format.createRecordReader(mockSplit, mockTAContext));
        verify(format).makeInputConf();
        verify(conf).from(rawConf);
    }
}