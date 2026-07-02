package org.sv.flexobject.hadoop.mapreduce.input.key;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.sv.flexobject.util.InstanceFactory;

import static org.junit.Assert.assertSame;

public class KeyInputFormatTest {

    @AfterEach
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