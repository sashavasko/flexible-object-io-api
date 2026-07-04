package org.sv.flexobject.hadoop.adapter;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.sv.flexobject.properties.Namespace;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConfigurationOutAdapterTest {

    @Test
    public void writesValuesToCurrentConfiguration() throws Exception {
        Configuration configuration = new Configuration(false);

        ConfigurationOutAdapter adapter = ConfigurationOutAdapter.builder()
                .to(configuration)
                .build();

        assertEquals("value", adapter.put("plain.key", "value"));
        adapter.setInt("max.retries", 7);
        adapter.setBoolean("enabled", true);

        assertEquals("value", configuration.get("plain.key"));
        assertEquals("7", configuration.get("max.retries"));
        assertEquals(7, configuration.getInt("max.retries", 0));
        assertEquals("true", configuration.get("enabled"));
        assertTrue(configuration.getBoolean("enabled", false));
    }

    @Test
    public void nullValueUnsetsExistingConfigurationKey() throws Exception {
        Configuration configuration = new Configuration(false);
        configuration.set("plain.key", "old-value");

        ConfigurationOutAdapter adapter = ConfigurationOutAdapter.builder()
                .to(configuration)
                .build();

        assertNull(adapter.put("plain.key", null));

        assertNull(configuration.get("plain.key"));
    }

    @Test
    public void translatorConvertsCamelCaseAndAppliesNamespace() throws Exception {
        Configuration configuration = new Configuration(false);
        Namespace namespace = new Namespace("service", ".");

        ConfigurationOutAdapter adapter = ConfigurationOutAdapter.builder()
                .to(configuration)
                .translator(namespace.getTranslator())
                .build();

        assertEquals("service.max.retries", adapter.translateOutputFieldName("maxRetries"));

        adapter.setInt("maxRetries", 7);
        adapter.setBoolean("enabled", true);

        assertEquals("7", configuration.get("service.max.retries"));
        assertEquals("true", configuration.get("service.enabled"));
        assertNull(configuration.get("maxRetries"));
        assertNull(configuration.get("enabled"));
    }

    @Test
    public void saveWritesCurrentConfigurationToDefaultSink() throws Exception {
        Configuration configuration = new Configuration(false);

        ConfigurationOutAdapter adapter = ConfigurationOutAdapter.builder()
                .to(configuration)
                .build();

        assertTrue(adapter.shouldSave());

        adapter.setString("plain.key", "value");
        adapter.save();

        assertFalse(adapter.shouldSave());
        assertTrue(adapter.hasOutput());
        assertSame(configuration, adapter.getSink().get());
        assertFalse(adapter.hasOutput());
    }

    @Test
    public void updateAppliesNamespaceTranslatorToConsumerWrites() throws Exception {
        Configuration configuration = new Configuration(false);
        Namespace namespace = new Namespace("client", ".");

        ConfigurationOutAdapter.update(configuration, namespace, adapter -> {
            adapter.setString("endpointUrl", "https://example.test");
            adapter.setInt("requestTimeoutMillis", 5000);
        });

        assertEquals("https://example.test", configuration.get("client.endpoint.url"));
        assertEquals("5000", configuration.get("client.request.timeout.millis"));
        assertNull(configuration.get("endpointUrl"));
        assertNull(configuration.get("requestTimeoutMillis"));
    }
}
