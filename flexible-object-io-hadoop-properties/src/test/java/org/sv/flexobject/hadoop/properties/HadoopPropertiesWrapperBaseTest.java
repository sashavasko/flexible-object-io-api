package org.sv.flexobject.hadoop.properties;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.sv.flexobject.InAdapter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HadoopPropertiesWrapperBaseTest {

    @Test
    public void fromLoadsNamespacedConfigurationAndStoresConf() {
        Configuration configuration = new Configuration(false);
        configuration.set("sv.client.max.retries", "7");
        configuration.set("sv.client.endpoint.url", "https://example.test");
        configuration.set("sv.client.enabled", "true");
        configuration.set("max.retries", "99");

        TestWrapper wrapper = new TestWrapper();

        assertSame(wrapper, wrapper.from(configuration));

        assertSame(configuration, wrapper.getConf());
        assertEquals(7, wrapper.maxRetries);
        assertEquals("https://example.test", wrapper.endpointUrl);
        assertTrue(wrapper.enabled);
    }

    @Test
    public void fromPreservesDefaultsForMissingConfigurationValues() {
        Configuration configuration = new Configuration(false);
        configuration.set("sv.client.endpoint.url", "https://example.test");

        TestWrapper wrapper = new TestWrapper().from(configuration);

        assertEquals(3, wrapper.maxRetries);
        assertEquals("https://example.test", wrapper.endpointUrl);
        assertFalse(wrapper.enabled);
    }

    @Test
    public void fromNullReturnsThisAndDoesNotSetConf() {
        TestWrapper wrapper = new TestWrapper();

        assertSame(wrapper, wrapper.from((Configuration) null));

        assertNull(wrapper.getConf());
    }

    @Test
    public void updateWritesNamespacedPropertiesToConfiguration() throws Exception {
        TestWrapper wrapper = new TestWrapper();
        wrapper.maxRetries = 9;
        wrapper.endpointUrl = "https://update.example.test";
        wrapper.enabled = true;
        Configuration configuration = new Configuration(false);

        assertTrue(wrapper.update(configuration));

        assertEquals("9", configuration.get("sv.client.max.retries"));
        assertEquals("https://update.example.test", configuration.get("sv.client.endpoint.url"));
        assertEquals("true", configuration.get("sv.client.enabled"));
        assertNull(configuration.get("maxRetries"));
        assertNull(configuration.get("endpointUrl"));
    }

    @Test
    public void setConfDelegatesToFrom() {
        Configuration configuration = new Configuration(false);
        configuration.set("sv.client.max.retries", "11");

        TestWrapper wrapper = new TestWrapper();
        wrapper.setConf(configuration);

        assertSame(configuration, wrapper.getConf());
        assertEquals(11, wrapper.maxRetries);
    }

    @Test
    public void addDiagnosticsIncludesMessageClassAndConfigurationState() {
        TestWrapper wrapper = new TestWrapper();
        wrapper.endpointUrl = "https://diagnostics.example.test";

        String diagnostics = wrapper.addDiagnostics("Failed to build client");

        assertTrue(diagnostics.startsWith("Failed to build client in Configuration "));
        assertTrue(diagnostics.contains(TestWrapper.class.getName()));
        assertTrue(diagnostics.contains("https://diagnostics.example.test"));
    }

    @Test
    public void runtimeExceptionWrapsCheckedExceptionsWithDiagnostics() {
        TestWrapper wrapper = new TestWrapper();
        Exception cause = new Exception("checked failure");

        RuntimeException exception = wrapper.runtimeException("Failed", cause);

        assertSame(cause, exception.getCause());
        assertTrue(exception.getMessage().contains("Failed in Configuration "));
        assertTrue(exception.getMessage().contains(TestWrapper.class.getName()));
    }

    @Test
    public void runtimeExceptionReturnsRuntimeExceptionsUnchanged() {
        TestWrapper wrapper = new TestWrapper();
        RuntimeException original = new RuntimeException("already runtime");

        RuntimeException exception = wrapper.runtimeException("Failed", original);

        assertSame(original, exception);
    }

    @Test
    public void fromWrapsLoadFailuresWithConfigurationDiagnostics() {
        Configuration configuration = new Configuration(false);
        FailingWrapper wrapper = new FailingWrapper();

        RuntimeException exception = assertThrows(RuntimeException.class, () -> wrapper.from(configuration));

        assertEquals("Configuration failed with exception in Configuration " + FailingWrapper.class.getName() + "{FailingWrapper{}}", exception.getMessage());
        assertEquals("load failed", exception.getCause().getMessage());
    }

    @SuppressWarnings("unchecked")
    public static class TestWrapper extends HadoopPropertiesWrapperBase<TestWrapper> {
        public Integer maxRetries;
        public String endpointUrl;
        public Boolean enabled;

        public TestWrapper() {
            super("client");
        }

        @Override
        public TestWrapper setDefaults() {
            maxRetries = 3;
            enabled = false;
            return this;
        }

        @Override
        public String toString() {
            return "TestWrapper{" +
                    "maxRetries=" + maxRetries +
                    ", endpointUrl='" + endpointUrl + '\'' +
                    ", enabled=" + enabled +
                    '}';
        }
    }

    @SuppressWarnings("unchecked")
    public static class FailingWrapper extends HadoopPropertiesWrapperBase<FailingWrapper> {

        @Override
        public boolean load(InAdapter input) throws Exception {
            throw new Exception("load failed");
        }

        @Override
        public String toString() {
            return "FailingWrapper{}";
        }
    }
}
