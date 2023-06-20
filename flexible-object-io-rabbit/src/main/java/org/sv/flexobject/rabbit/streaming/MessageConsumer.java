package org.sv.flexobject.rabbit.streaming;

import org.sv.flexobject.connections.ConnectionManager;
import org.sv.flexobject.rabbit.RabbitConnectionFactory;
import org.sv.flexobject.rabbit.RabbitException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MessageConsumer implements AutoCloseable {
    Connection connection;
    boolean ownConnection;
    Channel channel;
    List<MessageListenerSink> consumers = new ArrayList<>();


    public static class Builder {
        String connectionName;
        RabbitConnectionFactory connectionFactory;
        Connection connection;
        ListenerSinkFactory sinkFactory;
        boolean autoStart = true;
        int prefetchCount = 20;
        int txSize = 20;
        List<String> queueNames = new ArrayList<>();
        Integer concurrentConsumers;
        String consumerTag = "";

        public Builder forConnection(String connectionName){
            this.connectionName = connectionName;
            return this;
        }

        public Builder forConnection(RabbitConnectionFactory connectionFactory){
            this.connectionFactory = connectionFactory;
            return this;
        }

        public Builder forConnection(Connection connection){
            this.connection = connection;
            return this;
        }

        public Builder target(ListenerSinkFactory sinkFactory){
            this.sinkFactory = sinkFactory;
            return this;
        }

        public Builder tag(String consumerTag){
            this.consumerTag = consumerTag;
            return this;
        }

        public Builder noAutoStart(){
            this.autoStart = false;
            return this;
        }

        public Builder prefetchCount(int prefetchCount){
            this.prefetchCount = prefetchCount;
            return this;
        }

        public Builder batchSize(int batchSize){
            this.txSize = batchSize;
            return this;
        }

        public Builder txSize(int batchSize){
            this.txSize = batchSize;
            return this;
        }

        public Builder queues(String queueNames){
            return queues(queueNames.split(","));
        }

        public Builder queues(String ... queueNames){
            this.queueNames = Arrays.asList(queueNames);
            return this;
        }

        public Builder addQueue(String queue){
            this.queueNames.add(queue);
            return this;
        }

        public Builder concurrentConsumers(int concurrentConsumers){
            this.concurrentConsumers = concurrentConsumers;
            return this;
        }

        protected Connection getConnection() {
            if (connection != null)
                return connection;

            try {
                if (connectionFactory == null){
                    return (Connection) ConnectionManager.getConnection(Connection.class, connectionName);
                }
                return connectionFactory.getConnection();
            } catch (Exception e) {
                throw new RabbitException("Failed to establish Rabbit connection named \"" + connectionName +"\"",  e);
            }
        }

        protected int calculateConcurrentConsumers(){
            if (concurrentConsumers != null)
                return concurrentConsumers;
            if (queueNames.contains("monitoring"))
                return 1;
            return 6;
        }

        public MessageConsumer build() throws IOException {
            MessageConsumer messageConsumer = new MessageConsumer();
            messageConsumer.ownConnection = (connection == null);
            messageConsumer.connection = getConnection();
            messageConsumer.channel = messageConsumer.connection.createChannel();
            for (int i = 0 ; i < calculateConcurrentConsumers() ; ++i){
                MessageListenerSink sink = sinkFactory.get();
                for (String queueName : queueNames)
                    sink.bind(messageConsumer.channel, queueName, consumerTag);
                messageConsumer.consumers.add(sink);
            }
            return messageConsumer;
        }
    }
    
    public static Builder builder() {
        return new Builder();
    }

    public Connection getConnection() {
        return connection;
    }

    public Channel getChannel() {
        return channel;
    }

    public List<MessageListenerSink> getConsumers() {
        return consumers;
    }

    @Override
    public void close() throws Exception {
        for (MessageListenerSink sink : consumers){
            sink.setEOF();
        }
        if (channel != null) {
            channel.close();
            channel = null;
        }
        if (ownConnection && connection != null) {
            connection.close();
            connection = null;
        }
    }
}
