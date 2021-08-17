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
import org.sv.flexobject.stream.Source;
import org.sv.flexobject.stream.report.SizeReporter;
import org.sv.flexobject.util.InstanceFactory;

import java.io.IOException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class SourceRecordReaderTest {

    @Mock
    InputSplit mockSplit;

    @Mock
    TaskAttemptContext mockContext;

    @Mock
    SourceBuilder mockSourceBuilder;

    @Mock(extraInterfaces = {SizeReporter.class})
    Source mockSource;

    @Mock
    InstantiationException mockInstantiationException;

    @Mock
    IllegalAccessException mockIllegalAccessException;

    @Mock
    Object mockValue;

    @Mock
    Object mockKey;

    Configuration rawConf;
    SourceRecordReader reader;

    @Before
    public void setUp() throws Exception {
        rawConf = new Configuration(false);

        doReturn(rawConf).when(mockContext).getConfiguration();
        doReturn(mockSource).when(mockSourceBuilder).build(mockSplit, mockContext);

        InstanceFactory.set(String.class, mockSourceBuilder);
        rawConf.set("org.sv.flexobject.input.source.builder.class", String.class.getName());

        reader = Mockito.mock(SourceRecordReader.class, Mockito.CALLS_REAL_METHODS);
    }

    @After
    public void tearDown() throws Exception {
        InstanceFactory.reset();
    }

    @Test
    public void createSource() throws IOException, IllegalAccessException, InstantiationException {
        assertSame(mockSource, reader.createSource(mockSplit, mockContext));

        doThrow(mockInstantiationException).when(mockSourceBuilder).build(mockSplit, mockContext);

        try{
            reader.createSource(mockSplit, mockContext);
            throw new RuntimeException("Should have thrown");
        } catch (IOException e){
            assertSame(mockInstantiationException, e.getCause());
            assertTrue(e.getMessage().startsWith("Failed to build source"));
        }

        doThrow(mockIllegalAccessException).when(mockSourceBuilder).build(mockSplit, mockContext);

        try{
            reader.createSource(mockSplit, mockContext);
            throw new RuntimeException("Should have thrown");
        } catch (IOException e){
            assertSame(mockIllegalAccessException, e.getCause());
            assertTrue(e.getMessage().startsWith("Failed to build source"));
        }
    }

    @Test
    public void setupInput() throws IOException {
        doReturn(777l).when((SizeReporter)mockSource).getSize();

        reader.setupInput(mockSplit, mockContext);
        assertSame(mockSource, reader.getSource());
        assertEquals(777l, reader.getProgressReporter().getSize());
    }

    @Test
    public void nextKeyValue() throws Exception {
        doReturn(2l).when((SizeReporter)mockSource).getSize();

        reader.setupInput(mockSplit, mockContext);

        doReturn(true).when(mockSource).isEOF();

        assertFalse(reader.nextKeyValue());

        doReturn(false).when(mockSource).isEOF();
        doReturn(mockValue).when(mockSource).get();

        assertTrue(reader.nextKeyValue());
        assertSame(mockValue, reader.convertCurrentValue());
        assertEquals(0.5f, reader.getProgress(), 0.05);

        doThrow(mockIllegalAccessException).when(mockSource).get();

        try{
            reader.nextKeyValue();
            throw new RuntimeException("Should have thrown");
        }catch (IOException e) {
            assertSame(mockIllegalAccessException, e.getCause());
            assertEquals("Failed to get next Value", e.getMessage());
        }

    }

    @Test
    public void convertCurrentKeyValue() throws Exception {
        reader.setupInput(mockSplit, mockContext);

        doReturn(false).when(mockSource).isEOF();
        doReturn(mockValue).when(mockSource).get();
        doReturn(mockKey).when(reader).extractKeyFromValue(mockValue);

        assertTrue(reader.nextKeyValue());

        assertSame(mockKey, reader.convertCurrentKey());
        assertSame(mockValue, reader.convertCurrentValue());
    }

    @Test
    public void close() throws Exception {
        reader.setupInput(mockSplit, mockContext);

        reader.close();

        verify(mockSource).close();
    }

    @Test
    public void closeWithIOException() throws Exception {
        // mockito goes crazy when you just mock out IOException
        IOException testIOException = new IOException("test");

        doThrow(testIOException).when(mockSource).close();
        reader.setupInput(mockSplit, mockContext);
        try{
            reader.close();
            throw new RuntimeException("Should have thrown");
        }catch (IOException e){
            assertSame(testIOException, e);
        }

    }

    @Test
    public void closeWithNonIOException() throws Exception {
        doThrow(mockIllegalAccessException).when(mockSource).close();
        reader.setupInput(mockSplit, mockContext);
        try{
            reader.close();
            throw new RuntimeException("Should have thrown");
        }catch (IOException e){
            assertSame(mockIllegalAccessException, e.getCause());
            assertEquals("Failed to close source", e.getMessage());
        }
    }
}