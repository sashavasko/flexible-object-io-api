package org.sv.flexobject.hadoop.mapreduce.input.batch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.sv.flexobject.properties.Namespace;
import org.sv.flexobject.util.InstanceFactory;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(MockitoExtension.class)
public class BatchSplitterTest {

    BatchInputConf conf;

    Configuration rawConf;

    BatchSplitter splitter;

    @BeforeEach
    public void setUp() throws Exception {
        splitter = new BatchSplitter();
        conf = new BatchInputConf(new Namespace("test", "."));
        rawConf = new Configuration(false);
        InstanceFactory.set(BatchInputConf.class, conf);
    }

    @AfterEach
    public void tearDown() throws Exception {
        InstanceFactory.reset();
    }

    @Test
    public void splitUsesConf() throws IOException {
        rawConf.setLong("test.batch.batches.num", 5l);
        rawConf.setInt("test.batch.batches.per.split", 5);

        splitter.split(rawConf);

        assertEquals(1l, conf.getSplitsCount());
    }

    @Test
    public void split() throws IOException {
        rawConf.setLong("test.batch.key.start", 100l);
        rawConf.setInt("test.batch.batches.num", 9);
        rawConf.setInt("test.batch.size", 100);
        rawConf.setInt("test.batch.batches.per.split", 4);

        List<InputSplit> splits = splitter.split(rawConf);

        assertEquals(3, splits.size());
        assertEquals(4, ((BatchInputSplit)splits.get(0)).getBatchPerSplit());
        assertEquals(4, ((BatchInputSplit)splits.get(1)).getBatchPerSplit());
        assertEquals(1, ((BatchInputSplit)splits.get(2)).getBatchPerSplit());

        assertEquals(100, ((BatchInputSplit)splits.get(0)).getBatchSize());
        assertEquals(100, ((BatchInputSplit)splits.get(1)).getBatchSize());
        assertEquals(100, ((BatchInputSplit)splits.get(2)).getBatchSize());

        assertEquals(100l, ((BatchInputSplit)splits.get(0)).getStartKey());
        assertEquals(500l, ((BatchInputSplit)splits.get(1)).getStartKey());
        assertEquals(900l, ((BatchInputSplit)splits.get(2)).getStartKey());
    }
}