package org.sv.flexobject.hadoop.mapreduce.input.batch;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.sv.flexobject.properties.Namespace;
import org.sv.flexobject.schema.Schema;
import org.sv.flexobject.schema.SchemaElement;
import org.sv.flexobject.util.InstanceFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class BatchInputConfTest {

    @Mock
    BatchInputSplit split;
    @Mock MaxKeyCalculator maxKeyCalculator;
    @Mock BatchKeyManager keyManager;
    @Mock
    Configuration mockRawConf;

    BatchInputConf conf;

    @Before
    public void setUp() throws Exception {
        conf = new BatchInputConf();
    }

    @After
    public void tearDown() throws Exception {
        InstanceFactory.reset();
    }

    @Test
    public void listSettings() {
        List<String> expectedSettings = Arrays.asList(
                "sv.input.batch.splitter.class",
                "sv.input.batch.reader.class",
                "sv.input.batch.source.builder.class",
                "sv.input.batch.key.start",
                "sv.input.batch.size",
                "sv.input.batch.batches.per.split",
                "sv.input.batch.batches.num",
                "sv.input.batch.split.class",
                "sv.input.batch.key.max.dataset.path",
                "sv.input.batch.key.max.dataset.column.name",
                "sv.input.batch.key.max.calculator",
                "sv.input.batch.key.manager",
                "sv.input.batch.key.column.name",
                "sv.input.batch.reduce.max.keys");
        List<String> actualSettings = conf.listSettings();

        assertEquals(expectedSettings, actualSettings);
    }

    @Test
    public void setDefaults() {
        conf.setDefaults();
        assertEquals(0l, conf.getKeyStart());
        assertEquals(1000, conf.getSize());
        assertEquals(200, conf.getBatchesPerSplit());
        assertEquals(15000, conf.getBatchesNum());
        assertSame(BatchInputSplit.class, conf.getSplitClass());
        assertSame(BatchSplitter.class, conf.getSplitterClass());
        assertSame(BatchRecordReader.Long.class, conf.getReaderClass());
        assertSame(ParquetMaxKeyCalculator.class, conf.getKeyMaxCalculatorClass());
        assertSame(BatchKeyManager.class, conf.getKeyManagerClass());
        assertEquals(16000000l, conf.getReduceMaxKeys());
    }

    @Test
    public void namespace() {
        conf = new BatchInputConf(Namespace.forPath(".", "test", "input"));
        Configuration rawConf = new Configuration(false);
        rawConf.setInt("test.input.batch.size", 12345);

        conf.from(rawConf);

        assertEquals(12345, conf.getSize());
    }

    @Test
    public void subnamespace() {
        assertEquals("sv.input.batch", conf.getNamespace().toString());
    }

    @Test
    public void setters() {
        conf.configureMaxKeyDataset("foo", "bar");

        assertEquals("foo", conf.getKeyMaxDatasetPath());
        assertEquals("bar", conf.getKeyMaxDatasetColumnName());

        conf.setKeyColumnName("blah");

        assertEquals("blah", conf.getKeyColumnName());

        conf.setKeyStart(777l);

        assertEquals(777l, conf.getKeyStart());

        conf.setSize(898989);

        assertEquals(898989, conf.getSize());

        conf.setBatchesNum(6767);

        assertEquals(6767, conf.getBatchesNum());
    }

    @Test
    public void getKeyManager() {
        InstanceFactory.set(BatchKeyManager.class, keyManager);
        conf.setConf(mockRawConf);

        assertSame(keyManager, conf.getKeyManager());

        verify(keyManager).setConf(mockRawConf);
    }

    @Test
    public void getKeyMaxCalculator() {
        InstanceFactory.set(ParquetMaxKeyCalculator.class, maxKeyCalculator);
        conf.setConf(mockRawConf);

        assertSame(maxKeyCalculator, conf.getKeyMaxCalculator());

        verify(maxKeyCalculator).setConf(mockRawConf);
    }

    @Test
    public void getSplit() {
        InstanceFactory.set(BatchInputSplit.class, split);
        conf.setConf(mockRawConf);

        assertSame(split, conf.getSplit());

        verify(split).setConf(mockRawConf);
    }

    @Test
    public void getSplitsCount() {
        conf = new BatchInputConf(Namespace.forPath(".", "test"));
        Configuration rawConf = new Configuration(false);
        rawConf.setInt("test.batch.batches.num", 60);
        rawConf.setInt("test.batch.batches.per.split", 11);

        conf.from(rawConf);

        assertEquals(70/11, conf.getSplitsCount());
    }

    @Test
    public void getMaxKey() {
        conf = new BatchInputConf(Namespace.forPath(".", "test"));
        Configuration rawConf = new Configuration(false);
        rawConf.setLong("test.batch.key.start", 17l);
        rawConf.setInt("test.batch.batches.num", 60);
        rawConf.setInt("test.batch.size", 11);

        conf.from(rawConf);

        assertEquals(17l + 660l, conf.getMaxKey());
    }
}