package org.sv.flexobject.hadoop.mapreduce.input.batch;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.sv.flexobject.properties.Namespace;
import org.sv.flexobject.util.InstanceFactory;

import java.io.*;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class BatchInputSplitTest {

    Configuration rawConf;
    BatchInputConf conf;

    BatchInputSplit split;

    @Before
    public void setUp() throws Exception {
        rawConf = new Configuration(false);
        conf = new BatchInputConf(Namespace.forPath(".", "test"));
        split = new BatchInputSplit();
        InstanceFactory.set(BatchInputConf.class, conf);
    }

    @After
    public void tearDown() throws Exception {
        InstanceFactory.reset();
    }

    @Test
    public void reconfigure() {
        rawConf.setInt("test.batch.size", 12345);
        rawConf.setInt("test.batch.batches.per.split", 789);

        split.setConf(rawConf);

        assertEquals(12345, split.getBatchSize());
        assertEquals(789, split.getBatchPerSplit());
        assertEquals(789l, split.getLength());
    }

    @Test
    public void setGetStartKey() {
        split.setStartKey(787878787l);

        assertEquals(787878787l, split.getStartKey());
    }

    @Test
    public void getLocations() {
        assertArrayEquals(new String[0], split.getLocations());
    }

    @Test
    public void writeRead() throws IOException {
        rawConf.setInt("test.batch.size", 345);
        rawConf.setInt("test.batch.batches.per.split", 89);

        split.setConf(rawConf);
        split.setStartKey(676767l);
        assertEquals(Long.valueOf(676767l).hashCode(), split.hashCode());

        ByteArrayOutputStream output = new ByteArrayOutputStream();

        split.write(new DataOutputStream(output));

        BatchInputSplit converted = new BatchInputSplit();

        converted.readFields(new DataInputStream(new ByteArrayInputStream(output.toByteArray())));

        assertEquals(split, converted);
    }
}