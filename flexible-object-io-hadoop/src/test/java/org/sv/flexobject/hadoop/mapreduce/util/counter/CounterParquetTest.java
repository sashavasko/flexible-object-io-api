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
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class CounterParquetTest {

    Configuration configuration = new Configuration(false);
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
        configuration.set("sv.hadoop.parquet.counters", "test_CounterParquet");
        configuration.set("fs.defaultFS", "file:///", "file:///");
        configuration.setInt("file.bytes-per-checksum", 512);
        configuration.setInt("io.file.buffer.size", 4096);
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

        Mockito.verifyNoInteractions(context);
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
    }

    @Test
    public void closeUsesPathObjectForConfiguredLocation() throws Exception {
        counter.increment("foo", 5);

        counter.close();
    }
}