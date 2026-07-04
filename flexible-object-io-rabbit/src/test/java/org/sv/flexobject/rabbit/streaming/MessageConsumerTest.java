package org.sv.flexobject.rabbit.streaming;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import org.junit.jupiter.api.Test;
import org.sv.flexobject.rabbit.Message;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

public class MessageConsumerTest {

    @Test
    public void builderCreatesDefaultConcurrentConsumersAndBindsQueues() throws Exception {
        Connection connection = mock(Connection.class);
        Channel channel = mock(Channel.class);
        when(connection.createChannel()).thenReturn(channel);
        TestSinkFactory sinkFactory = new TestSinkFactory();

        MessageConsumer consumer = MessageConsumer.builder()
                .forConnection(connection)
                .target(sinkFactory)
                .tag("consumer")
                .queues("queue-a", "queue-b")
                .build();

        assertSame(connection, consumer.getConnection());
        assertSame(channel, consumer.getChannel());
        assertEquals(6, consumer.getConsumers().size());
        assertFalse(consumer.ownConnection);
        verify(channel, times(6)).basicConsume(eq("queue-a"), eq(false), eq("consumer"), any(Consumer.class));
        verify(channel, times(6)).basicConsume(eq("queue-b"), eq(false), eq("consumer"), any(Consumer.class));
        assertEquals(6, sinkFactory.sinks.size());
        for (TestSink sink : sinkFactory.sinks) {
            assertEquals(2, sink.bindCount);
        }
    }

    @Test
    public void monitoringQueueUsesSingleConsumerByDefault() throws Exception {
        Connection connection = mock(Connection.class);
        when(connection.createChannel()).thenReturn(mock(Channel.class));
        TestSinkFactory sinkFactory = new TestSinkFactory();

        MessageConsumer consumer = MessageConsumer.builder()
                .forConnection(connection)
                .target(sinkFactory)
                .addQueue("monitoring")
                .build();

        assertEquals(1, consumer.getConsumers().size());
    }

    @Test
    public void explicitConcurrentConsumersOverridesDefault() throws Exception {
        Connection connection = mock(Connection.class);
        when(connection.createChannel()).thenReturn(mock(Channel.class));
        TestSinkFactory sinkFactory = new TestSinkFactory();

        MessageConsumer consumer = MessageConsumer.builder()
                .forConnection(connection)
                .target(sinkFactory)
                .queues("queue")
                .concurrentConsumers(2)
                .build();

        assertEquals(2, consumer.getConsumers().size());
    }

    @Test
    public void closeClosesSinksChannelAndOwnedConnection() throws Exception {
        Connection connection = mock(Connection.class);
        Channel channel = mock(Channel.class);
        when(connection.createChannel()).thenReturn(channel);
        TestSinkFactory sinkFactory = new TestSinkFactory();
        MessageConsumer consumer = MessageConsumer.builder()
                .forConnection(connection)
                .target(sinkFactory)
                .queues("queue")
                .concurrentConsumers(2)
                .build();
        consumer.ownConnection = true;

        consumer.close();

        assertTrue(sinkFactory.sinks.get(0).eof);
        assertTrue(sinkFactory.sinks.get(1).eof);
        verify(channel).close();
        verify(connection).close();
    }

    static class TestSinkFactory implements ListenerSinkFactory {
        List<TestSink> sinks = new ArrayList<>();

        @Override
        public MessageListenerSink get() {
            TestSink sink = new TestSink();
            sinks.add(sink);
            return sink;
        }
    }

    static class TestSink extends MessageListenerFinalSink<TestSink> {
        int bindCount;
        boolean eof;

        @Override
        public void bind(Channel channel, String queueName, String consumerTag) throws java.io.IOException {
            bindCount++;
            super.bind(channel, queueName, consumerTag);
        }

        @Override
        public boolean put(Message value) {
            return true;
        }

        @Override
        public void setEOF() {
            eof = true;
        }

        @Override
        public boolean hasOutput() {
            return false;
        }
    }
}
