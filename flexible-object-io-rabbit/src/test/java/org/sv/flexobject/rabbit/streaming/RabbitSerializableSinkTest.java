package org.sv.flexobject.rabbit.streaming;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.util.zip.InflaterInputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RabbitSerializableSinkTest {

    @Test
    public void deflateCompressesBytes() {
        byte[] input = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".getBytes();

        byte[] output = RabbitSerializableSink.deflate(input, input.length);

        assertTrue(output.length > 0);
        assertTrue(output.length < input.length);
    }

    @Test
    public void encodeMessageSerializesAndCompressesObject() throws Exception {
        byte[] encoded = RabbitSerializableSink.encodeMessage("payload");

        assertEquals("payload", inflateAndDeserialize(encoded));
    }

    @Test
    public void builderCreatesSerializableSink() throws Exception {
        RabbitGenericSink.Builder builder = RabbitSerializableSink.builder();

        RabbitSerializableSink sink = builder
                .forConnection(org.mockito.Mockito.mock(com.rabbitmq.client.Connection.class))
                .toExchange("exchange")
                .routeUsing(new RoutingKeyGenerator.ConstantString("key"))
                .build();

        assertTrue(sink instanceof RabbitSerializableSink);
    }

    private Object inflateAndDeserialize(byte[] encoded) throws Exception {
        try (InflaterInputStream inflater = new InflaterInputStream(new ByteArrayInputStream(encoded));
             ByteArrayOutputStream inflated = new ByteArrayOutputStream()) {
            inflater.transferTo(inflated);
            try (ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(inflated.toByteArray()))) {
                return input.readObject();
            }
        }
    }
}
