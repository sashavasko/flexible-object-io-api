package org.sv.flexobject.hadoop.mapreduce.util.counter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class CounterParquetTest {

    @Mock
    Configuration configuration;

    @Mock
    TaskInputOutputContext context;

    @Mock
    TaskAttemptID taskId;

    CounterParquet counter = new CounterParquet("Testing");

    @Before
    public void setUp() throws Exception {
        Mockito.when(context.getConfiguration()).thenReturn(configuration);
        Mockito.when(context.getTaskAttemptID()).thenReturn(taskId);
        Mockito.when(taskId.toString()).thenReturn("testTask");
        Mockito.when(configuration.get("org.sv.flexobject.hadoop.parquet.counters")).thenReturn("test_CounterParquet");
        Mockito.when(configuration.get("fs.defaultFS", "file:///")).thenReturn("file:///");
        Mockito.when(configuration.getInt("file.bytes-per-checksum", 512)).thenReturn(512);
        Mockito.when(configuration.getInt("io.file.buffer.size", 4096)).thenReturn(4096);
        counter.setContext(context);
    }

    @After
    public void tearDown() throws Exception {
        counter.close();

        Path testFilepath = new Path("test_CounterParquet");
        testFilepath.getFileSystem(configuration).delete(testFilepath, true);
    }

    @Test
    public void increment() {
        assertEquals(0l, counter.getCounter("foo"));

        counter.increment("foo", 77);

        assertEquals(77l, counter.getCounter("foo"));

        counter.increment("foo", 5);

        assertEquals(82l, counter.getCounter("foo"));
    }

    @Test
    public void closeWithNoCountersDoesNothing() throws Exception {
        counter.close();

        Mockito.verifyZeroInteractions(context);
    }


    @Test
    public void closeClearsCounters() throws Exception {

        counter.increment("foo", 5);

        counter.close();

        assertEquals(0l, counter.getCounter("foo"));
    }

    @Test
    public void closeUsesConfigurationForCountersLocation() throws Exception {
        counter.increment("foo", 5);

        counter.close();

        Mockito.verify(configuration).get("org.sv.flexobject.hadoop.parquet.counters");
    }

    @Test
    public void closeUsesPathObjectForConfiguredLocation() throws Exception {
        counter.increment("foo", 5);

        counter.close();

        Mockito.verify(configuration).get("org.sv.flexobject.hadoop.parquet.counters");
    }
}