package org.sv.flexobject.hadoop;

import com.carfax.dt.streaming.schema.Schema;
import com.carfax.dt.streaming.schema.SchemaElement;
import com.carfax.hadoop.properties.HadoopPropertiesProvider;
import com.carfax.hadoop.properties.HadoopSecretProvider;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class HadoopTaskConfTest {

    @Test
    public void listSettings() {
        HadoopTaskConf conf = new HadoopTaskConf();
        List<String> expectedSettings = Arrays.asList(
                "cfx.hadoop.db.environment",
                "cfx.hadoop.deployment.level",
                "cfx.hadoop.connection.manager.providers",
                "cfx.hadoop.connection.manager.environment");
        List<String> actualSettings = new ArrayList<>();

        for (SchemaElement e : Schema.getRegisteredSchema(conf.getClass()).getFields()){
            actualSettings.add(conf.getSettingName(e.getDescriptor().getName()));
        }

        assertEquals(expectedSettings, actualSettings);
    }

    @Test
    public void providerClasses() {
        Configuration configuration = new Configuration(false);
        configuration.set("cfx.hadoop.connection.manager.providers", HadoopSecretProvider.class.getName() + "," + HadoopPropertiesProvider.class.getName());
        HadoopTaskConf conf = new HadoopTaskConf();

        conf.from(configuration);
        List<Class> expectedProviders = Arrays.asList(HadoopSecretProvider.class, HadoopPropertiesProvider.class);
        assertEquals(expectedProviders, conf.getConnectionManagerProviders());
    }
}