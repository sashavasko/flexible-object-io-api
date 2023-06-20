package org.sv.flexobject.rabbit;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class RabbitConnectionConfTest {
    @Test
    public void listSettings() {
        RabbitConnectionConf conf = new RabbitConnectionConf();
        List<String> expectedSettings = Arrays.asList(
                "host",
                "virtualHost",
                "uri",
                "username",
                "password",
                "port",
                "addresses",
                "clientProviderName",
                "automaticRecoveryEnabled",
                "recoveryIntervalMillis",
                "executorService");
        List<String> actualSettings = conf.listSettings();

        assertEquals(expectedSettings, actualSettings);
    }
}