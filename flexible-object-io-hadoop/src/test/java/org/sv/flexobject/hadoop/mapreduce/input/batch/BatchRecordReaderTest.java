package org.sv.flexobject.hadoop.mapreduce.input.batch;

import org.apache.hadoop.io.LongWritable;
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
import org.sv.flexobject.stream.report.ProgressReporter;
import org.sv.flexobject.util.InstanceFactory;

import java.io.IOException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class BatchRecordReaderTest {

    @Mock
    AdapterRecordReader.LongField key;

    @Mock
    LongWritable longWritable;

    @Mock
    BatchInputSplit split;

    @Mock
    TaskAttemptContext context;

    @Mock
    ProgressReporter progressReporter;

    @Mock
    BatchInputDao inputDao;

    @Mock
    InAdapter adapter;

    @Mock
    InAdapter adapter2;

    @Mock
    InAdapter adapter3;

    @Mock
    InAdapter adapter4;

    @Mock
    AdapterRecordReader.TextField textField;

    @Mock
    AdapterRecordReader.LongField longField;

    BatchRecordReader reader;

    @Before
    public void setUp() throws Exception {
        reader = Mockito.mock(BatchRecordReader.class, Mockito.CALLS_REAL_METHODS);

        doReturn(key).when(reader).longField();
        doReturn(progressReporter).when(reader).getProgressReporter();
        doReturn(inputDao).when(reader).getDao();
    }

    @After
    public void tearDown() throws Exception {
        InstanceFactory.reset();
    }

    @Test
    public void convertCurrentKey() throws Exception {
        doReturn("foo").when(reader).getKeyFieldName();
        doReturn(longWritable).when(key).convert("foo");

        assertEquals(longWritable, reader.convertCurrentKey());
    }

    @Test
    public void createAdapter() throws IOException {
        assertNull(reader.createAdapter(split, context));
    }

    @Test
    public void initialize() throws IOException, InterruptedException {
        doNothing().when(reader).initializeSuper(split, context);
        doReturn(true).when(reader).nextBatch();
        doReturn(777l).when(split).getStartKey();
        doReturn(10l).when(split).getBatchPerSplit();
        doReturn(split).when(reader).getSplit();

        reader.initialize(split, context);

        assertEquals(777l, reader.getNextBatchStartKey());
        assertTrue(reader.hasData());

        reader.noMoreData();

        assertFalse(reader.hasData());

        verify(reader).initializeSuper(split, context);
        verify(progressReporter).setSize(10l);
    }

    @Test
    public void incrementBatch() {
        assertEquals(0, reader.getBatchNo());

        reader.incrementBatch();

        assertEquals(1, reader.getBatchNo());
        verify(progressReporter).increment();
    }

    @Test
    public void nextBatch() throws Exception {
        doNothing().when(reader).initializeSuper(split, context);
        doReturn(100l).when(split).getBatchSize();
        doReturn(777l).when(split).getStartKey();
        doReturn(6l).when(split).getBatchPerSplit();
        doReturn(split).when(reader).getSplit();

        doReturn(adapter).when(inputDao).startBatch(777l, 100l);
        when(reader.isEmptyBatch()).thenReturn(false, false, false, true, true);

        reader.initialize(split, context);

        assertTrue(reader.hasData());

        verify(reader).setInput(adapter);

//        doReturn(900l).when(inputDao).adjustStartKey(877l, 977l);
        doReturn(adapter2).when(inputDao).startBatch(877l, 100l);

        assertTrue(reader.nextBatch());

        verify(reader).setInput(adapter2);

        doReturn(adapter3).when(inputDao).startBatch(977l, 100l);

        assertTrue(reader.nextBatch());

        verify(reader).setInput(adapter3);

        doReturn(1180l).when(inputDao).adjustStartKey(1077l, 1377l);
        doReturn(adapter4).when(inputDao).startBatch(1177l, 100l);

        assertTrue(reader.nextBatch());

        verify(inputDao).adjustStartKey(1077l, 1377l);
        verify(reader).setInput(adapter4);

        doReturn(1500l).when(inputDao).adjustStartKey(1277l, 1377l);

        assertFalse(reader.nextBatch());

        verify(reader,times(6)).incrementBatch();
    }

    @Test
    public void nextKeyValue() throws Exception {
        doReturn(false).when(reader).hasData();

        assertFalse(reader.nextKeyValue());

        doReturn(adapter).when(reader).getInput();
        doReturn(true).when(reader).hasData();
        doReturn(true).when(adapter).next();

        assertTrue(reader.nextKeyValue());
        verify(reader).nextRecordFound();

        doReturn(true).when(reader).hasData();
        doReturn(false).when(adapter).next();
        doReturn(false).when(reader).hasMoreBatches();

        assertFalse(reader.nextKeyValue());

        doReturn(true).when(reader).hasData();
        doReturn(false).when(adapter).next();
        doReturn(true).when(reader).hasMoreBatches();
        doReturn(false).when(reader).nextBatch();

        assertFalse(reader.nextKeyValue());
        verify(reader).noMoreData();
    }

    @Test
    public void nextKeyValueInNextBatch() throws Exception {
        doReturn(adapter).when(reader).getInput();

        doReturn(true).when(reader).hasData();
        when(adapter.next()).thenReturn(false, true);
        doReturn(true).doReturn(true).when(reader).hasMoreBatches();
        doReturn(true).when(reader).nextBatch();

        assertTrue(reader.nextKeyValue());
        verify(reader).nextRecordFound();
    }

    @Test
    public void nextKeyValueInNextBatchWithEmptyBatch() throws Exception {
        doReturn(adapter).when(reader).getInput();

        doReturn(true).when(reader).hasData();
        when(adapter.next()).thenReturn(false, false, true);
        doReturn(true).doReturn(true).when(reader).hasMoreBatches();
        doReturn(true).doReturn(true).when(reader).nextBatch();

        assertTrue(reader.nextKeyValue());
        verify(reader).nextRecordFound();
    }

    @Test
    public void nextKeyValueInNextBatchWithEmptyBatchAndNoMore() throws Exception {
        doReturn(adapter).when(reader).getInput();

        doReturn(true).when(reader).hasData();
        when(adapter.next()).thenReturn(false, false);
        doReturn(true).doReturn(false).when(reader).hasMoreBatches();
        doReturn(true).when(reader).nextBatch();

        assertFalse(reader.nextKeyValue());
    }

    @Test
    public void nextKeyValueInNextBatchWithEmptyBatchAndLastBatchFails() throws Exception {
        doReturn(adapter).when(reader).getInput();

        doReturn(true).when(reader).hasData();
        when(adapter.next()).thenReturn(false, false, true);
        doReturn(true).doReturn(true).when(reader).hasMoreBatches();
        doReturn(true).doReturn(false).when(reader).nextBatch();

        assertFalse(reader.nextKeyValue());
        verify(reader).noMoreData();
    }

    @Test
    public void text() throws Exception {
        BatchRecordReader.Text reader = Mockito.mock(BatchRecordReader.Text.class, Mockito.CALLS_REAL_METHODS);

        doReturn(textField).when(reader).textField();
        doReturn("foo").when(reader).getValueFieldName();

        reader.convertCurrentValue();

        verify(textField).convert("foo");
    }

    @Test
    public void longReader() throws Exception {
        BatchRecordReader.Long reader = Mockito.mock(BatchRecordReader.Long.class, Mockito.CALLS_REAL_METHODS);

        doReturn(longField).when(reader).longField();
        doReturn("foo").when(reader).getValueFieldName();

        reader.convertCurrentValue();

        verify(longField).convert("foo");
    }
}