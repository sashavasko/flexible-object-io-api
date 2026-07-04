package org.sv.flexobject.rabbit;

import com.rabbitmq.client.Address;
import org.junit.jupiter.api.Test;
import org.sv.flexobject.util.InstanceFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    @Test
    public void defaults() {
        RabbitConnectionConf conf = new RabbitConnectionConf();

        assertEquals(5672, conf.getPort());
        assertTrue(conf.isAutomaticRecovery());
        assertEquals(6000, conf.getRecoveryIntervalMillis());
        assertFalse(conf.hasURI());
        assertFalse(conf.hasAddresses());
        assertFalse(conf.hasExecutorService());
        assertNull(conf.getAddresses());
        assertNull(conf.getUri());
    }

    @Test
    public void constructorSetsHostPortAndUsername() {
        RabbitConnectionConf conf = new RabbitConnectionConf("rabbit.local", 5673, "guest");

        assertEquals("rabbit.local", conf.getHost());
        assertEquals(5673, conf.getPort());
        assertEquals("guest", conf.getUsername());
    }

    @Test
    public void fromPropertiesLoadsAllFields() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("host", "rabbit.local");
        properties.setProperty("virtualHost", "/vhost");
        properties.setProperty("uri", "amqp://guest:guest@rabbit.local:5672/%2f");
        properties.setProperty("username", "guest");
        properties.setProperty("password", "password");
        properties.setProperty("port", "5673");
        properties.setProperty("addresses", "rabbit-a:5674,rabbit-b:5675");
        properties.setProperty("clientProviderName", "client-name");
        properties.setProperty("automaticRecoveryEnabled", "false");
        properties.setProperty("recoveryIntervalMillis", "12000");

        RabbitConnectionConf conf = new RabbitConnectionConf().from(properties);

        assertEquals("rabbit.local", conf.getHost());
        assertEquals("/vhost", conf.getVirtualHost());
        assertEquals(URI.create("amqp://guest:guest@rabbit.local:5672/%2f"), conf.getUri());
        assertEquals("guest", conf.getUsername());
        assertEquals("password", conf.getPassword());
        assertEquals(5673, conf.getPort());
        assertEquals("client-name", conf.getClientProviderName());
        assertFalse(conf.isAutomaticRecovery());
        assertEquals(12000, conf.getRecoveryIntervalMillis());

        Address[] addresses = conf.getAddresses();
        assertEquals(2, addresses.length);
        assertEquals("rabbit-a", addresses[0].getHost());
        assertEquals(5674, addresses[0].getPort());
        assertEquals("rabbit-b", addresses[1].getHost());
        assertEquals(5675, addresses[1].getPort());
    }

    @Test
    public void hostCreatesSingleAddressWhenAddressesAreMissing() {
        RabbitConnectionConf conf = new RabbitConnectionConf("rabbit.local", 5678, "guest");

        assertTrue(conf.hasAddresses());

        Address[] addresses = conf.getAddresses();
        assertEquals(1, addresses.length);
        assertEquals("rabbit.local", addresses[0].getHost());
        assertEquals(5678, addresses[0].getPort());
    }

    @Test
    public void explicitAddressesTakePrecedenceOverHost() {
        RabbitConnectionConf conf = new RabbitConnectionConf("rabbit.local", 5678, "guest");
        conf.addresses = "rabbit-a:5674";

        Address[] addresses = conf.getAddresses();

        assertEquals(1, addresses.length);
        assertEquals("rabbit-a", addresses[0].getHost());
        assertEquals(5674, addresses[0].getPort());
    }

    @Test
    public void recoveryIntervalFallsBackWhenZero() {
        RabbitConnectionConf conf = new RabbitConnectionConf();
        conf.recoveryIntervalMillis = 0;

        assertEquals(6000, conf.getRecoveryIntervalMillis());
    }

    @Test
    public void executorServiceIsCreatedThroughInstanceFactory() {
        TestExecutorService executorService = new TestExecutorService();
        InstanceFactory.set(TestExecutorService.class, executorService);
        RabbitConnectionConf conf = new RabbitConnectionConf();
        conf.executorService = TestExecutorService.class;

        assertTrue(conf.hasExecutorService());
        assertEquals(TestExecutorService.class, conf.getExecutorServiceClass());
        assertSame(executorService, conf.getExecutorService());

        InstanceFactory.reset();
    }

    public static class TestExecutorService extends AbstractExecutorService {
        @Override
        public void shutdown() {
        }

        @Override
        public List<Runnable> shutdownNow() {
            return new ArrayList<>();
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return false;
        }

        @Override
        public void execute(Runnable command) {
            command.run();
        }
    }
}
