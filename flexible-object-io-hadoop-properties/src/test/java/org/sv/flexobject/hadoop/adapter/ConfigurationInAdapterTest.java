package org.sv.flexobject.hadoop.adapter;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.sv.flexobject.stream.sources.QueueSource;
import org.sv.flexobject.stream.sources.SingleValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConfigurationInAdapterTest {

    @Test
    public void getReturnsValuesFromCurrentConfiguration() throws Exception {
        Configuration configuration = new Configuration(false);
        configuration.set("plain.key", "value");

        ConfigurationInAdapter adapter = ConfigurationInAdapter.builder()
                .from(configuration)
                .build();

        assertTrue(adapter.next());
        assertEquals("value", adapter.get("plain.key"));
        assertEquals("value", adapter.getString("plain.key"));
        assertNull(adapter.get("missing.key"));
        assertFalse(adapter.next());
    }

    @Test
    public void getTranslatorConvertsCamelCaseAndAppliesNamespace() throws Exception {
        Configuration configuration = new Configuration(false);
        configuration.set("service.max.retries", "7");
        configuration.set("service.enabled", "true");

        ConfigurationInAdapter adapter = ConfigurationInAdapter.builder()
                .from(configuration)
                .translator(ConfigurationInAdapter.getTranslator("service"))
                .build();

        assertEquals("service.max.retries", adapter.translateInputFieldName("maxRetries"));

        assertTrue(adapter.next());
        assertEquals(7, adapter.getInt("maxRetries"));
        assertTrue(adapter.getBoolean("enabled"));
    }

    @Test
    public void constructorSetsSourceAndNamespaceTranslator() throws Exception {
        Configuration configuration = new Configuration(false);
        configuration.set("client.endpoint.url", "https://example.test");

        ConfigurationInAdapter adapter = new ConfigurationInAdapter(
                new SingleValueSource<>(configuration),
                "client"
        );

        assertEquals("client.endpoint.url", adapter.translateInputFieldName("endpointUrl"));
        assertTrue(adapter.next());
        assertEquals("https://example.test", adapter.getString("endpointUrl"));
    }

    @Test
    public void builderReadsFromProvidedSource() throws Exception {
        Configuration first = new Configuration(false);
        first.set("name", "first");
        Configuration second = new Configuration(false);
        second.set("name", "second");
        QueueSource<Configuration> source = new QueueSource<>(first, second);

        ConfigurationInAdapter adapter = ConfigurationInAdapter.builder()
                .fromSource(source)
                .build();

        assertSame(source, adapter.getSource());

        assertTrue(adapter.next());
        assertSame(first, adapter.getCurrent());
        assertEquals("first", adapter.getString("name"));

        assertTrue(adapter.next());
        assertSame(second, adapter.getCurrent());
        assertEquals("second", adapter.getString("name"));

        assertFalse(adapter.next());
    }
}
