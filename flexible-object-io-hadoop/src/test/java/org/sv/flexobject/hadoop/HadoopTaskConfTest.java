package org.sv.flexobject.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.sv.flexobject.hadoop.properties.HadoopPropertiesProvider;
import org.sv.flexobject.hadoop.properties.HadoopSecretProvider;
import org.sv.flexobject.schema.Schema;
import org.sv.flexobject.schema.SchemaElement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class HadoopTaskConfTest {

    @Test
    public void listSettings() {
        HadoopTaskConf conf = new HadoopTaskConf();
        List<String> expectedSettings = Arrays.asList(
                "org.sv.flexobject.hadoop.db.environment",
                "org.sv.flexobject.hadoop.deployment.level",
                "org.sv.flexobject.hadoop.connection.manager.providers",
                "org.sv.flexobject.hadoop.connection.manager.environment");
        List<String> actualSettings = new ArrayList<>();

        for (SchemaElement e : Schema.getRegisteredSchema(conf.getClass()).getFields()){
            actualSettings.add(conf.getSettingName(e.getDescriptor().getName()));
        }

        assertEquals(expectedSettings, actualSettings);
    }

    @Test
    public void providerClasses() {
        Configuration configuration = new Configuration(false);
        configuration.set("org.sv.flexobject.hadoop.connection.manager.providers", HadoopSecretProvider.class.getName() + "," + HadoopPropertiesProvider.class.getName());
        HadoopTaskConf conf = new HadoopTaskConf();

        conf.from(configuration);
        List<Class> expectedProviders = Arrays.asList(HadoopSecretProvider.class, HadoopPropertiesProvider.class);
        assertEquals(expectedProviders, conf.getConnectionManagerProviders());
    }
}