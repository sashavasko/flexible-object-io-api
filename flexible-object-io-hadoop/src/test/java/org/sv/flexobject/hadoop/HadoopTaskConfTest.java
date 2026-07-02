package org.sv.flexobject.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.sv.flexobject.hadoop.properties.HadoopPropertiesProvider;
import org.sv.flexobject.hadoop.properties.HadoopSecretProvider;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class HadoopTaskConfTest {

    @Test
    public void listSettings() {
        HadoopTaskConf conf = new HadoopTaskConf();
        List<String> expectedSettings = Arrays.asList(
                "sv.hadoop.db.environment",
                "sv.hadoop.deployment.level",
                "sv.hadoop.connection.manager.providers",
                "sv.hadoop.connection.manager.environment",
                "sv.hadoop.tool.class",
                "sv.hadoop.conf.class");
        List<String> actualSettings = conf.listSettings();

        assertEquals(expectedSettings, actualSettings);
    }

    @Test
    public void providerClasses() {
        Configuration configuration = new Configuration(false);
        configuration.set("sv.hadoop.connection.manager.providers", HadoopSecretProvider.class.getName() + "," + HadoopPropertiesProvider.class.getName());
        HadoopTaskConf conf = new HadoopTaskConf();

        conf.from(configuration);
        List<Class> expectedProviders = Arrays.asList(HadoopSecretProvider.class, HadoopPropertiesProvider.class);
        assertEquals(expectedProviders, conf.getConnectionManagerProviders());
    }
}