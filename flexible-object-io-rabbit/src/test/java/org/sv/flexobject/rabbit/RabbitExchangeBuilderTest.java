package org.sv.flexobject.rabbit;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RabbitExchangeBuilderTest {

    @Test
    public void buildDeclaresExchangeQueuesAndBindings() throws Exception {
        Connection connection = mock(Connection.class);
        Channel channel = mock(Channel.class);
        Map<String, Object> arguments = Map.of("x-message-ttl", 1000);
        when(connection.createChannel()).thenReturn(channel);

        RabbitExchangeBuilder.forConnection(connection)
                .name("exchange")
                .type(BuiltinExchangeType.TOPIC)
                .addQueue("queue")
                .durable()
                .exclusive()
                .autoDelete()
                .arguments(arguments)
                .forKey("routing.key")
                .build();

        verify(channel).exchangeDeclare("exchange", BuiltinExchangeType.TOPIC);
        verify(channel).queueDeclare("queue", true, true, true, arguments);
        verify(channel).queueBind("queue", "exchange", "routing.key");
        verify(channel).close();
    }

    @Test
    public void buildWrapsTimeoutException() throws Exception {
        Connection connection = mock(Connection.class);
        Channel channel = mock(Channel.class);
        when(connection.createChannel()).thenReturn(channel);
        doThrow(new TimeoutException("timeout")).when(channel).close();

        RabbitException exception = assertThrows(
                RabbitException.class,
                () -> RabbitExchangeBuilder.forConnection(connection).name("exchange").build()
        );

        assertEquals("Failed to build exchange exchange", exception.getMessage());
    }
}
