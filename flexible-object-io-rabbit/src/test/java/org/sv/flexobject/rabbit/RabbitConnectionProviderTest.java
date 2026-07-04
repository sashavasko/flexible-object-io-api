package org.sv.flexobject.rabbit;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.sv.flexobject.util.InstanceFactory;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RabbitConnectionProviderTest {

    @AfterEach
    public void tearDown() throws Exception {
        cachedConnectionFactories().clear();
        InstanceFactory.reset();
    }

    @Test
    public void factoryProviderAppliesPropertiesAndSecret() throws Exception {
        RabbitConnectionFactory rabbitConnectionFactory = mock(RabbitConnectionFactory.class);
        InstanceFactory.set(RabbitConnectionFactory.class, rabbitConnectionFactory);
        RabbitConnectionFactoryProvider provider = new RabbitConnectionFactoryProvider();
        Properties properties = new Properties();

        AutoCloseable connection = provider.getConnection("rabbit", properties, "secret");

        assertSame(rabbitConnectionFactory, connection);
        verify(rabbitConnectionFactory).applyProperties(properties);
        verify(rabbitConnectionFactory).setPassword("secret");
    }

    @Test
    public void factoryProviderListsRabbitConnectionFactoryType() {
        Iterator<Class<? extends AutoCloseable>> types =
                new RabbitConnectionFactoryProvider().listConnectionTypes().iterator();

        assertEquals(RabbitConnectionFactory.class, types.next());
    }

    @Test
    public void connectionProviderListsRabbitConnectionType() {
        Iterator<Class<? extends AutoCloseable>> types =
                new RabbitConnectionProvider().listConnectionTypes().iterator();

        assertEquals(Connection.class, types.next());
    }

    @Test
    public void getConnectionUsesAddressesAndCachesFactoryWhenRecoveryIsEnabled() throws Exception {
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
        Connection connection = mock(Connection.class);
        when(connectionFactory.newConnection(any(Address[].class), eq("client"))).thenReturn(connection);
        InstanceFactory.set(ConnectionFactory.class, connectionFactory);
        RabbitConnectionConf conf = new RabbitConnectionConf("rabbit.local", 5672, "guest");
        conf.clientProviderName = "client";
        RabbitConnectionProvider provider = new RabbitConnectionProvider();

        Connection first = provider.getConnection("cache-enabled", conf, "secret");
        Connection second = provider.getConnection("cache-enabled", conf, "secret");

        assertSame(connection, first);
        assertSame(connection, second);
        verify(connectionFactory).setUsername("guest");
        verify(connectionFactory).setPassword("secret");
        verify(connectionFactory).setAutomaticRecoveryEnabled(true);
        verify(connectionFactory).setNetworkRecoveryInterval(6000L);
        verify(connectionFactory, times(2)).newConnection(any(Address[].class), eq("client"));
    }

    @Test
    public void getConnectionUsesExecutorServiceWhenConfigured() throws Exception {
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
        Connection connection = mock(Connection.class);
        RabbitConnectionConfTest.TestExecutorService executorService = new RabbitConnectionConfTest.TestExecutorService();
        Address[] addresses = new Address[]{new Address("rabbit.local", 5672)};
        when(connectionFactory.newConnection(eq(executorService), any(Address[].class), eq("client"))).thenReturn(connection);
        InstanceFactory.set(ConnectionFactory.class, connectionFactory);
        InstanceFactory.set(RabbitConnectionConfTest.TestExecutorService.class, executorService);
        RabbitConnectionConf conf = new RabbitConnectionConf("rabbit.local", 5672, "guest");
        conf.executorService = RabbitConnectionConfTest.TestExecutorService.class;
        conf.clientProviderName = "client";

        Connection actual = new RabbitConnectionProvider().getConnection("with-executor", conf, "secret");

        assertSame(connection, actual);
        verify(connectionFactory).newConnection(eq(executorService), any(Address[].class), eq("client"));
    }

    @Test
    public void getConnectionUsesUriAndClientNameWhenNoAddresses() throws Exception {
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
        Connection connection = mock(Connection.class);
        when(connectionFactory.newConnection("client")).thenReturn(connection);
        InstanceFactory.set(ConnectionFactory.class, connectionFactory);
        RabbitConnectionConf conf = new RabbitConnectionConf();
        conf.username = "guest";
        conf.uri = "amqp://guest:guest@rabbit.local:5672/%2f";
        conf.clientProviderName = "client";

        Connection actual = new RabbitConnectionProvider().getConnection("uri-only", conf, "secret");

        assertSame(connection, actual);
        verify(connectionFactory).setUri(conf.getUri());
        verify(connectionFactory).newConnection("client");
    }

    @Test
    public void getConnectionDoesNotCacheFactoryWhenRecoveryIsDisabled() throws Exception {
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
        Connection connection = mock(Connection.class);
        when(connectionFactory.newConnection("client")).thenReturn(connection);
        InstanceFactory.set(ConnectionFactory.class, connectionFactory);
        RabbitConnectionConf conf = new RabbitConnectionConf();
        conf.username = "guest";
        conf.clientProviderName = "client";
        conf.automaticRecoveryEnabled = false;

        Connection actual = new RabbitConnectionProvider().getConnection("no-cache", conf, "secret");

        assertSame(connection, actual);
        assertTrue(cachedConnectionFactories().isEmpty());
    }

    @Test
    public void getConnectionFromPropertiesLoadsConfiguration() throws Exception {
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
        Connection connection = mock(Connection.class);
        when(connectionFactory.newConnection(any(Address[].class), eq("client"))).thenReturn(connection);
        InstanceFactory.set(ConnectionFactory.class, connectionFactory);
        Properties properties = new Properties();
        properties.setProperty("host", "rabbit.local");
        properties.setProperty("username", "guest");
        properties.setProperty("clientProviderName", "client");

        AutoCloseable actual = new RabbitConnectionProvider().getConnection("from-properties", properties, "secret");

        assertSame(connection, actual);
        verify(connectionFactory).setUsername("guest");
    }

    @Test
    public void getConnectionWrapsConnectionFailures() throws Exception {
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
        when(connectionFactory.newConnection("client")).thenThrow(new IOException("boom"));
        InstanceFactory.set(ConnectionFactory.class, connectionFactory);
        RabbitConnectionConf conf = new RabbitConnectionConf();
        conf.username = "guest";
        conf.clientProviderName = "client";

        RabbitException exception = assertThrows(
                RabbitException.class,
                () -> new RabbitConnectionProvider().getConnection("failure", conf, "secret")
        );

        assertTrue(exception.getMessage().contains("Failed to connect to Rabbit using configuration"));
        assertEquals("boom", exception.getCause().getMessage());
    }

    @SuppressWarnings("unchecked")
    private Map<String, ConnectionFactory> cachedConnectionFactories() throws Exception {
        Field field = RabbitConnectionProvider.class.getDeclaredField("connectionFactories");
        field.setAccessible(true);
        return (Map<String, ConnectionFactory>) field.get(null);
    }
}
