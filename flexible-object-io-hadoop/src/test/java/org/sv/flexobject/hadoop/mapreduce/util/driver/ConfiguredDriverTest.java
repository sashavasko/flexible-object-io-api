package org.sv.flexobject.hadoop.mapreduce.util.driver;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

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