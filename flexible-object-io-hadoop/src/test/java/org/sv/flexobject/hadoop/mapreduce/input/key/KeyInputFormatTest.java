package org.sv.flexobject.hadoop.mapreduce.input.key;

import org.junit.After;
import org.junit.Test;
import org.sv.flexobject.util.InstanceFactory;

import static org.junit.Assert.assertSame;

public class KeyInputFormatTest {

    @After
    public void tearDown() throws Exception {
        InstanceFactory.reset();
    }

    @Test
    public void makeInputConf() {
        KeyInputFormat format = new KeyInputFormat();
        KeyInputConf conf = new KeyInputConf();
        InstanceFactory.set(KeyInputConf.class, conf);

        assertSame(conf, format.makeInputConf());
    }
}