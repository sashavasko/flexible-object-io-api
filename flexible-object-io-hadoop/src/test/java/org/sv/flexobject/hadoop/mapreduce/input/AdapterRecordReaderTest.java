package org.sv.flexobject.hadoop.mapreduce.input;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.sv.flexobject.InAdapter;
import org.sv.flexobject.stream.report.ProgressReporter;
import org.sv.flexobject.stream.report.SizeReporter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class AdapterRecordReaderTest {

    @Mock(extraInterfaces = SizeReporter.class)
    InAdapter mockAdapter1;

    @Mock(extraInterfaces = SizeReporter.class)
    InAdapter mockAdapter2;

    @Mock
    ProgressReporter mockReporter;

    @Mock
    Logger mockLogger;

    @Captor
    ArgumentCaptor<String> stringCaptor;

    @Captor
    ArgumentCaptor<Exception> exceptionCaptor;

    AdapterRecordReader reader;

    @Before
    public void setUp() throws Exception {
        reader = Mockito.mock(AdapterRecordReader.class, Mockito.CALLS_REAL_METHODS);
        reader.setProgressReporter(mockReporter);
        doReturn(100l).when((SizeReporter)mockAdapter1).getSize();
        doReturn(200l).when((SizeReporter)mockAdapter2).getSize();
    }

    @Test
    public void setGetInput() throws Exception {
        reader.setInput(mockAdapter1);

        assertSame(mockAdapter1, reader.getInput());
        verify(mockReporter).setSize(mockAdapter1);

        reader.setInput(mockAdapter2);

        assertSame(mockAdapter2, reader.getInput());
        verify(mockReporter).setSize(mockAdapter2);
        verify(mockAdapter1).close();
    }

    @Test
    public void setGetInputLogsException() throws Exception {
        doThrow(Exception.class).when(mockAdapter1).close();
        reader.logger = mockLogger;
        reader.setInput(mockAdapter1);

        reader.setInput(mockAdapter2);
        verify(mockLogger).error(stringCaptor.capture(), exceptionCaptor.capture());
        assertEquals("Failed to close previous Input Adapter", stringCaptor.getValue());
    }

    @Test
    public void close() throws Exception {
        reader.close();

        reader.setInput(mockAdapter1);

        reader.close();

        verify(mockAdapter1).close();

        doThrow(Exception.class).when(mockAdapter2).close();
        reader.logger = mockLogger;
        reader.setInput(mockAdapter2);
        reader.close();

        verify(mockLogger).error(stringCaptor.capture(), exceptionCaptor.capture());
        assertEquals("Failed to close reader", stringCaptor.getValue());
    }

    @Test
    public void longField() throws Exception {
        AdapterRecordReader.LongField longField = reader.longField();

        reader.setInput(mockAdapter1);
        doReturn(1234567l).when(mockAdapter1).getLong("field");

        assertEquals(new LongWritable(1234567l), longField.convert("field"));
    }

    @Test
    public void textField() throws Exception {
        AdapterRecordReader.TextField field = reader.textField();

        reader.setInput(mockAdapter1);
        doReturn("foobar").when(mockAdapter1).getString("field2");

        assertEquals(new Text("foobar"), field.convert("field2"));
    }
}