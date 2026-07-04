package org.sv.flexobject.rabbit;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;

public class MessageTest {

    @Test
    public void gettersReturnConstructorValues() {
        Envelope envelope = mock(Envelope.class);
        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                .contentType("application/json")
                .build();
        byte[] body = "payload".getBytes();

        Message message = new Message("consumer", envelope, properties, body);

        assertSame(envelope, message.getEnvelope());
        assertSame(properties, message.getProperties());
        assertArrayEquals(body, message.getBody());
    }
}
