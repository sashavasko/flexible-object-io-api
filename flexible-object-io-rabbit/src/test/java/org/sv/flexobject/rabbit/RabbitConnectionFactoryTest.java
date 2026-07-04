package org.sv.flexobject.rabbit;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.sv.flexobject.util.InstanceFactory;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.eq;

public class RabbitConnectionFactoryTest {

    @AfterEach
    public void tearDown() {
        InstanceFactory.reset();
    }

    @Test
    public void applyPropertiesConfiguresConnectionFactory() throws Exception {
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
        InstanceFactory.set(ConnectionFactory.class, connectionFactory);
        RabbitConnectionFactory rabbitConnectionFactory = new RabbitConnectionFactory();
        Properties properties = new Properties();
        properties.setProperty("host", "rabbit.local");
        properties.setProperty("port", "5673");
        properties.setProperty("username", "guest");
        properties.setProperty("password", "password");
        properties.setProperty("uri", "amqp://guest:guest@rabbit.local:5673/%2f");

        rabbitConnectionFactory.applyProperties(properties);

        verify(connectionFactory).setUsername("guest");
        verify(connectionFactory).setHost("rabbit.local");
        verify(connectionFactory).setPort(5673);
        verify(connectionFactory).setUri(eq(java.net.URI.create("amqp://guest:guest@rabbit.local:5673/%2f")));
        verify(connectionFactory).setPassword("password");
    }

    @Test
    public void setPasswordIgnoresNullSecret() {
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
        InstanceFactory.set(ConnectionFactory.class, connectionFactory);
        RabbitConnectionFactory rabbitConnectionFactory = new RabbitConnectionFactory();

        rabbitConnectionFactory.setPassword(null);

        verifyNoMoreInteractions(connectionFactory);
    }

    @Test
    public void getConnectionDelegatesToConnectionFactory() throws Exception {
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
        Connection connection = mock(Connection.class);
        when(connectionFactory.newConnection()).thenReturn(connection);
        InstanceFactory.set(ConnectionFactory.class, connectionFactory);
        RabbitConnectionFactory rabbitConnectionFactory = new RabbitConnectionFactory();

        assertSame(connection, rabbitConnectionFactory.getConnection());
    }

    @Test
    public void getConnectionWithAddressesDelegatesToConnectionFactory() throws Exception {
        ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
        Connection connection = mock(Connection.class);
        Address[] addresses = new Address[]{new Address("rabbit.local", 5672)};
        when(connectionFactory.newConnection(addresses)).thenReturn(connection);
        InstanceFactory.set(ConnectionFactory.class, connectionFactory);
        RabbitConnectionFactory rabbitConnectionFactory = new RabbitConnectionFactory();

        assertSame(connection, rabbitConnectionFactory.getConnection(addresses));
    }
}
