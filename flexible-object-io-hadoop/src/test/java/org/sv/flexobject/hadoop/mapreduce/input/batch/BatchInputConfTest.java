package org.sv.flexobject.hadoop.mapreduce.input.batch;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
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
                "org.sv.flexobject.input.batch.splitter.class",
                "org.sv.flexobject.input.batch.reader.class",
                "org.sv.flexobject.input.batch.source.builder.class",
                "org.sv.flexobject.input.batch.key.start",
                "org.sv.flexobject.input.batch.size",
                "org.sv.flexobject.input.batch.batches.per.split",
                "org.sv.flexobject.input.batch.batches.num",
                "org.sv.flexobject.input.batch.split.class",
                "org.sv.flexobject.input.batch.key.max.dataset.path",
                "org.sv.flexobject.input.batch.key.max.dataset.column.name",
                "org.sv.flexobject.input.batch.key.max.calculator",
                "org.sv.flexobject.input.batch.key.manager",
                "org.sv.flexobject.input.batch.key.column.name",
                "org.sv.flexobject.input.batch.reduce.max.keys");
        List<String> actualSettings = new ArrayList<>();

        for (SchemaElement e : Schema.getRegisteredSchema(conf.getClass()).getFields()){
            actualSettings.add(conf.getSettingName(e.getDescriptor().getName()));
        }

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
        conf = new BatchInputConf("test");
        Configuration rawConf = new Configuration(false);
        rawConf.setInt("test.input.batch.size", 12345);

        conf.from(rawConf);

        assertEquals(12345, conf.getSize());
    }

    @Test
    public void subnamespace() {
        assertEquals("input.batch", conf.getSubNamespace());
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
        conf =  Mockito.mock(BatchInputConf.class, Mockito.CALLS_REAL_METHODS);

        doReturn(60).when(conf).getBatchesNum();
        doReturn(11).when(conf).getBatchesPerSplit();

        assertEquals(70/11, conf.getSplitsCount());
    }

    @Test
    public void getMaxKey() {
        conf =  Mockito.mock(BatchInputConf.class, Mockito.CALLS_REAL_METHODS);

        doReturn(17l).when(conf).getKeyStart();
        doReturn(60).when(conf).getBatchesNum();
        doReturn(11).when(conf).getSize();

        assertEquals(17l + 660l, conf.getMaxKey());
    }

}