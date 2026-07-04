package org.sv.flexobject.rabbit.streaming;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.sv.flexobject.rabbit.RabbitException;
import org.sv.flexobject.util.InstanceFactory;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.AdditionalMatchers.aryEq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RabbitGenericSinkTest {

    @AfterEach
    public void tearDown() {
        InstanceFactory.reset();
    }

    @Test
    public void builderRequiresConnectionWhenConnectionNameAndFactoryAreMissing() {
        RabbitException exception = assertThrows(
                RabbitException.class,
                () -> RabbitGenericSink.builder()
                        .sinkType(RabbitGenericSink.class)
                        .toExchange("exchange")
                        .build()
        );

        assertEquals("Unable to build Rabbit Sink as connection is not specified", exception.getMessage());
    }

    @Test
    public void builderRequiresExchangeWhenUsingConnectionName() {
        RabbitException exception = assertThrows(
                RabbitException.class,
                () -> RabbitGenericSink.builder()
                        .sinkType(RabbitGenericSink.class)
                        .forConnection("rabbit")
                        .build()
        );

        assertEquals("Missing exchange for Rabbit Sink", exception.getMessage());
    }

    @Test
    public void builderUsesProvidedConnectionAndMessageProperties() throws Exception {
        Connection connection = mock(Connection.class);
        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                .contentEncoding("UTF-16")
                .type("payload.Type")
                .build();

        RabbitGenericSink<String> sink = RabbitGenericSink.builder()
                .sinkType(RabbitGenericSink.class)
                .forConnection(connection)
                .toExchange("exchange")
                .withProperties(properties)
                .routeUsing(new RoutingKeyGenerator.ConstantString("key"))
                .converter(value -> ((String) value).getBytes())
                .build();

        assertSame(connection, sink.getConnection());
        assertSame(properties, sink.messageProperties);
        assertFalse(sink.ownConnection);
    }

    @Test
    public void builderCreatesDefaultMessageProperties() throws Exception {
        Connection connection = mock(Connection.class);

        RabbitGenericSink<String> sink = RabbitGenericSink.builder()
                .sinkType(RabbitGenericSink.class)
                .forConnection(connection)
                .toExchange("exchange")
                .contentEncoding("UTF-8")
                .routeUsing(new RoutingKeyGenerator.ConstantString("key"))
                .converter(value -> ((String) value).getBytes())
                .build();

        assertEquals("UTF-8", sink.messageProperties.getContentEncoding());
    }

    @Test
    public void putPublishesConvertedMessage() throws Exception {
        Connection connection = mock(Connection.class);
        Channel channel = mock(Channel.class);
        when(connection.createChannel()).thenReturn(channel);
        RabbitGenericSink<String> sink = RabbitGenericSink.builder()
                .sinkType(RabbitGenericSink.class)
                .forConnection(connection)
                .toExchange("exchange")
                .routeUsing(new RoutingKeyGenerator.ConstantString("routing.key"))
                .converter(value -> ((String) value).getBytes())
                .build();

        assertTrue(sink.put("payload"));

        verify(channel).basicPublish(
                eq("exchange"),
                eq("routing.key"),
                eq(false),
                eq(false),
                any(AMQP.BasicProperties.class),
                aryEq("payload".getBytes())
        );
        assertTrue(sink.hasOutput());
    }

    @Test
    public void getChannelWrapsCreateChannelFailure() throws Exception {
        Connection connection = mock(Connection.class);
        when(connection.createChannel()).thenThrow(new IOException("boom"));
        RabbitGenericSink<String> sink = RabbitGenericSink.builder()
                .sinkType(RabbitGenericSink.class)
                .forConnection(connection)
                .toExchange("exchange")
                .routeUsing(new RoutingKeyGenerator.ConstantString("routing.key"))
                .converter(value -> ((String) value).getBytes())
                .build();

        RabbitException exception = assertThrows(RabbitException.class, sink::getChannel);

        assertEquals("Failed to creat Rabbit channel", exception.getMessage());
        assertEquals("boom", exception.getCause().getMessage());
    }

    @Test
    public void setEOFCleansUpChannelAndOwnedConnection() throws Exception {
        Connection connection = mock(Connection.class);
        Channel channel = mock(Channel.class);
        when(connection.createChannel()).thenReturn(channel);
        RabbitGenericSink<String> sink = RabbitGenericSink.builder()
                .sinkType(RabbitGenericSink.class)
                .forConnection(connection)
                .toExchange("exchange")
                .routeUsing(new RoutingKeyGenerator.ConstantString("routing.key"))
                .converter(value -> ((String) value).getBytes())
                .build();
        sink.ownConnection = true;
        sink.getChannel();

        sink.setEOF();

        verify(channel).close();
        verify(connection).close();
    }
}
