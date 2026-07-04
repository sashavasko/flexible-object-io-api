package org.sv.flexobject.rabbit.streaming;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Envelope;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.rabbit.Message;
import org.sv.flexobject.stream.sinks.SingleValueSink;
import org.sv.flexobject.util.InstanceFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class MessageListenerTransformTest {

    @AfterEach
    public void tearDown() {
        InstanceFactory.reset();
    }

    @Test
    public void bindConsumesWithoutAutoAckAndStoresChannel() throws Exception {
        Channel channel = mock(Channel.class);
        MessageListenerTransform<?> transform = new MessageListenerTransform<>();

        transform.bind(channel, "queue", "consumer");

        assertSame(channel, transform.getChannel());
        verify(channel).basicConsume("queue", false, "consumer", transform);
    }

    @Test
    public void consumeOkStoresConsumerTag() {
        MessageListenerTransform<?> transform = new MessageListenerTransform<>();

        transform.handleConsumeOk("consumer-tag");

        assertEquals("consumer-tag", transform.getLastConsumerTag());
    }

    @Test
    public void makeInstanceUsesMessageTypeWhenPresent() throws Exception {
        Message message = messageFor("{\"value\":\"typed\"}", TestPayload.class.getName(), null);
        MessageListenerTransform<?> transform = new MessageListenerTransform<>();

        assertTrue(transform.makeInstance(message) instanceof TestPayload);
    }

    @Test
    public void streamableJsonUsesDefaultTypeAndContentEncoding() throws Exception {
        Message message = messageFor("{\"value\":\"encoded\"}", null, "UTF-8");
        MessageListenerTransform<?> transform = new MessageListenerTransform<>();
        transform.setDefaultType(TestPayload.class);

        TestPayload payload = (TestPayload) transform.streamableJson(message);

        assertEquals("encoded", payload.value);
    }

    @Test
    public void putTransformsMessageIntoOutputSink() throws Exception {
        SingleValueSink<TestPayload> outputSink = new SingleValueSink<>();
        MessageListenerTransform<?> transform = new MessageListenerTransform<>(outputSink);
        transform.setDefaultType(TestPayload.class);

        assertTrue(transform.put(messageFor("{\"value\":\"sink\"}", null, null)));

        assertEquals("sink", outputSink.get().value);
    }

    @Test
    public void bufferingSinkFactoryCreatesTransformBackedBySharedBuffer() throws Exception {
        BufferingSinkFactory factory = new BufferingSinkFactory(TestPayload.class);
        MessageListenerSink sink = factory.get();

        sink.put(messageFor("{\"value\":\"buffered\"}", null, null));

        assertEquals(1, factory.getBuffer().size());
        assertEquals("buffered", ((TestPayload) factory.getBuffer().get(0)).value);
    }

    private Message messageFor(String json, String type, String contentEncoding) {
        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                .type(type)
                .contentEncoding(contentEncoding)
                .build();
        return new Message("consumer", new Envelope(1L, false, "exchange", "routing.key"), properties, json.getBytes());
    }

    public static class TestPayload extends StreamableWithSchema {
        public String value;
    }
}
