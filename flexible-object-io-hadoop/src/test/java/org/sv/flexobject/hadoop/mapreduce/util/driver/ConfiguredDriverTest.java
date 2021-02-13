package org.sv.flexobject.hadoop.mapreduce.util.driver;

import org.junit.Test;

import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

public class ConfiguredDriverTest {
    @Test
    public void prepareJob() throws Exception {
        ConfiguredDriver driver = new ConfiguredDriver() {
            @Override
            public void configureJob() {

            }
        };

        driver.prepareConfiguration(new String[]{"-conf", "someconffile"});

        assertTrue(driver.getConf().toString().contains("someconffile"));
    }
}