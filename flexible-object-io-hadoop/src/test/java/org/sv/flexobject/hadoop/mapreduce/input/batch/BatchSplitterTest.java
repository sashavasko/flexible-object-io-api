package org.sv.flexobject.hadoop.mapreduce.input.batch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;
import org.sv.flexobject.util.InstanceFactory;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class BatchSplitterTest {

    @Spy
    BatchInputConf conf;

    @Mock
    Configuration rawConf;

    BatchSplitter splitter;

    @Before
    public void setUp() throws Exception {
        splitter = new BatchSplitter();
        InstanceFactory.set(BatchInputConf.class, conf);
    }

    @After
    public void tearDown() throws Exception {
        InstanceFactory.reset();
    }

    @Test
    public void splitUsesConf() throws IOException {
        doReturn(1l).when(conf).getSplitsCount();
        splitter.split(rawConf);

        verify(conf, times(2)).from(rawConf);
    }

    @Test
    public void split() throws IOException {
        doReturn(1000l).when(conf).getMaxKey();
        doReturn(5l).when(conf).getKeyStart();
        doReturn(10).when(conf).getSize();
        doReturn(15).when(conf).getBatchesPerSplit();
        doReturn(7l).when(conf).getSplitsCount();


        List<InputSplit> splits = splitter.split(rawConf);

        assertEquals(7, splits.size());
        assertEquals(10, ((BatchInputSplit)splits.get(6)).getBatchPerSplit());
        for (int i = 0 ; i < 6 ; ++i )
            assertEquals(15, ((BatchInputSplit)splits.get(i)).getBatchPerSplit());
        for (int i = 0 ; i < 7 ; ++i )
            assertEquals(10, ((BatchInputSplit)splits.get(i)).getBatchSize());

        assertEquals(5l, ((BatchInputSplit)splits.get(0)).getStartKey());
        assertEquals(155l, ((BatchInputSplit)splits.get(1)).getStartKey());
        assertEquals(305l, ((BatchInputSplit)splits.get(2)).getStartKey());
        assertEquals(455l, ((BatchInputSplit)splits.get(3)).getStartKey());
        assertEquals(605l, ((BatchInputSplit)splits.get(4)).getStartKey());
        assertEquals(755l, ((BatchInputSplit)splits.get(5)).getStartKey());
        assertEquals(905l, ((BatchInputSplit)splits.get(6)).getStartKey());
    }

}