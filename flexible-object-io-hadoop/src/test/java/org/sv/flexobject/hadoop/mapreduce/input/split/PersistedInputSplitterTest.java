package org.sv.flexobject.hadoop.mapreduce.input.split;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sv.flexobject.hadoop.streaming.parquet.streamable.ParquetSink;

import java.util.List;

import static org.junit.Assert.*;

public class PersistedInputSplitterTest {

    Configuration rawConf;

    Path testFileFolder = new Path("test-splits");

    @Before
    public void setUp() throws Exception {
        rawConf = new Configuration(false);
    }

    @After
    public void tearDown() throws Exception {
        testFileFolder.getFileSystem(rawConf).delete(testFileFolder, true);
    }

    public static void writeTestFile(Configuration conf, Path testFilepath, long... values) throws Exception {
        try (ParquetSink sink = ParquetSink.builder()
                .forOutput(testFilepath)
                .withSchema(TestSplit.class)
                .withConf(conf).build()) {
            for (long value : values)
                sink.put(new TestSplit(value));
        }
    }

    @Test
    public void splitFromRealLocation() throws Exception {
        rawConf.set("cfx.input.splits.path", "test-splits");
        writeTestFile(rawConf, new Path(testFileFolder, "file1.parquet"), 5l, 10l, 15l, 20l);
        PersistedInputSplitter splitter = new PersistedInputSplitter();

        List<InputSplit> splits = splitter.split(rawConf);

        assertEquals(5l, splits.get(0).getLength());
        assertEquals(10l, splits.get(1).getLength());
        assertEquals(15l, splits.get(2).getLength());
        assertEquals(20l, splits.get(3).getLength());
    }
}