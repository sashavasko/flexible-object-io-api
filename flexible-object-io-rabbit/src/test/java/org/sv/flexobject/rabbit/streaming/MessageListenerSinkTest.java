package org.sv.flexobject.rabbit.streaming;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;
import org.junit.jupiter.api.Test;
import org.sv.flexobject.rabbit.Message;
import org.sv.flexobject.rabbit.RabbitException;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class MessageListenerSinkTest {

    @Test
    public void bindConsumesWithoutAutoAckAndStoresChannel() throws Exception {
        Channel channel = mock(Channel.class);
        TestSink sink = new TestSink();

        sink.bind(channel, "queue", "consumer");

        assertSame(channel, sink.getChannel());
        verify(channel).basicConsume("queue", false, "consumer", sink);
    }

    @Test
    public void handleDeliveryPutsMessageAndAcknowledgesDelivery() throws Exception {
        Channel channel = mock(Channel.class);
        TestSink sink = new TestSink();
        sink.bind(channel, "queue", "consumer");
        Envelope envelope = new Envelope(123L, false, "exchange", "routing.key");
        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                .contentType("application/json")
                .build();
        byte[] body = "payload".getBytes();

        sink.handleDelivery("consumer", envelope, properties, body);

        assertSame(envelope, sink.message.getEnvelope());
        assertSame(properties, sink.message.getProperties());
        assertArrayEquals(body, sink.message.getBody());
        verify(channel).basicAck(123L, false);
    }

    @Test
    public void handleDeliveryRethrowsIOExceptionBeforeAck() {
        TestSink sink = new TestSink();
        sink.exception = new IOException("put failed");
        Envelope envelope = new Envelope(123L, false, "exchange", "routing.key");

        IOException exception = assertThrows(
                IOException.class,
                () -> sink.handleDelivery("consumer", envelope, new AMQP.BasicProperties(), new byte[0])
        );

        assertEquals("put failed", exception.getMessage());
    }

    @Test
    public void handleDeliveryWrapsAckFailure() throws Exception {
        Channel channel = mock(Channel.class);
        doThrow(new IOException("ack failed")).when(channel).basicAck(123L, false);
        TestSink sink = new TestSink();
        sink.bind(channel, "queue", "consumer");
        Envelope envelope = new Envelope(123L, false, "exchange", "routing.key");

        RabbitException exception = assertThrows(
                RabbitException.class,
                () -> sink.handleDelivery("consumer", envelope, new AMQP.BasicProperties(), new byte[0])
        );

        assertEquals("Failed to ack the message", exception.getMessage());
        assertEquals("ack failed", exception.getCause().getMessage());
    }

    @Test
    public void handleConsumeOkStoresConsumerTag() {
        TestSink sink = new TestSink();

        sink.handleConsumeOk("consumer-tag");

        assertEquals("consumer-tag", sink.getLastConsumerTag());
    }

    static class TestSink extends MessageListenerFinalSink<TestSink> {
        Message message;
        Exception exception;

        @Override
        public boolean put(Message value) throws Exception {
            if (exception != null)
                throw exception;
            message = value;
            return true;
        }

        @Override
        public boolean hasOutput() {
            return message != null;
        }
    }
}
