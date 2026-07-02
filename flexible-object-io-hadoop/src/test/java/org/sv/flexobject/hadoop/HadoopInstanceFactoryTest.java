package org.sv.flexobject.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class HadoopInstanceFactoryTest {
    Configuration conf = new Configuration(false);

    public static class FooBar {
        public int foo;
        private String bar;
    }

    @BeforeEach
    public void setUp() throws Exception {
        HadoopInstanceFactory.setConf(conf);
    }

    @Test
    public void setConf() throws Exception {

        assertSame(conf, HadoopInstanceFactory.getConf());
    }

    @Test
    public void get() throws Exception {
        conf.set("class.property.name", FooBar.class.getName());
        FooBar i1 = (FooBar) HadoopInstanceFactory.get("class.property.name");
        FooBar i2 = (FooBar) HadoopInstanceFactory.get("class.property.name");
        assertTrue(i1 instanceof FooBar);
        assertTrue(i2 instanceof FooBar);
        assertNotSame(i1, i2);
    }

    @Test
    public void getSingleton() throws Exception {
        conf.set("class.property.name", FooBar.class.getName());
        FooBar i1 = (FooBar) HadoopInstanceFactory.getSingleton("class.property.name");
        FooBar i2 = (FooBar) HadoopInstanceFactory.getSingleton("class.property.name");
        assertTrue(i1 instanceof FooBar);
        assertTrue(i2 instanceof FooBar);
        assertSame(i1, i2);
    }

}