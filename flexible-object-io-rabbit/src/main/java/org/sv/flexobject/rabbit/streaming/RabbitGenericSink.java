package org.sv.flexobject.rabbit.streaming;

import org.sv.flexobject.Streamable;
import org.sv.flexobject.connections.ConnectionManager;
import org.sv.flexobject.rabbit.RabbitConnectionFactory;
import org.sv.flexobject.rabbit.RabbitException;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sv.flexobject.stream.Sink;
import org.sv.flexobject.util.FunctionWithException;
import org.sv.flexobject.util.InstanceFactory;

public class RabbitGenericSink<T> implements Sink<T>, AutoCloseable {

    public static final Logger logger = LogManager.getLogger(RabbitGenericSink.class);

    Connection connection;
    boolean ownConnection = false;
    Channel channel;
    String exchange;
    RoutingKeyGenerator routingKeyGenerator;
    boolean hasOutput = false;
    AMQP.BasicProperties messageProperties;

    FunctionWithException<T, byte[], Exception> converter;

    public static class Builder {
        Class <? extends RabbitGenericSink> sinkType;
        String connectionName;
        RabbitConnectionFactory connectionFactory;
        Connection connection;
        String exchange;
        Class type;

        RoutingKeyGenerator routingKeyGenerator;

        AMQP.BasicProperties messageProperties;

        FunctionWithException<?, byte[], Exception> converter;
        String contentEncoding;

        protected Builder sinkType(Class<? extends RabbitGenericSink> sinkType){
            this.sinkType = sinkType;
            return this;
        }

        public Builder forConnection(String connectionName){
            this.connectionName = connectionName;
            return this;
        }

        public Builder forConnection(Connection connection){
            this.connection = connection;
            return this;
        }

        public Builder forConnection(RabbitConnectionFactory connectionFactory){
            this.connectionFactory = connectionFactory;
            return this;
        }

        public Builder toExchange(String exchange){
            this.exchange = exchange;
            return this;
        }

        public Builder routeUsing(RoutingKeyGenerator routingKeyGenerator){
            this.routingKeyGenerator = routingKeyGenerator;
            return this;
        }

        public Builder withProperties(AMQP.BasicProperties properties){
            this.messageProperties = properties;
            return this;
        }

        public Builder type(Class<? extends Streamable> type){
            this.type = type;
            return this;
        }

        public Builder converter(FunctionWithException<?, byte[], Exception> converter){
            this.converter = converter;
            return this;
        }

        public Builder contentEncoding(String contentEncoding){
            this.contentEncoding = contentEncoding;
            return this;
        }

        public <S extends RabbitGenericSink> S build() throws Exception {
            if (connection == null) {
                if (connectionName == null && connectionFactory == null)
                    throw new RabbitException("Unable to build Rabbit Sink as connection is not specified");
                if (exchange == null)
                    throw new RabbitException("Missing exchange for Rabbit Sink");
            }

            RabbitGenericSink sink = InstanceFactory.get(sinkType);
            if (connection != null) {
                sink.connection = connection;
                sink.ownConnection = false;
            }else if (connectionFactory == null)
                sink.connection = (Connection) ConnectionManager.getConnection(Connection.class, connectionName);
            else
                sink.connection = connectionFactory.getConnection();
            sink.exchange = exchange;
            sink.converter = converter;
            if (messageProperties == null){
                AMQP.BasicProperties.Builder builder = MessageProperties.PERSISTENT_BASIC.builder();
                if (contentEncoding != null)
                    builder.contentEncoding(contentEncoding);
                if (type != null)
                    builder.type(type.getName());
                messageProperties = builder.build();
            }
            sink.messageProperties = this.messageProperties;
            sink.routingKeyGenerator = this.routingKeyGenerator;
            return (S) sinkType.cast(sink);
        }

    }

    public static Builder builder(){
        return InstanceFactory.get(Builder.class);
    }

    public Connection getConnection() {
        return connection;
    }

    public Channel getChannel(){
        if (channel == null) {
            try {
                channel = getConnection().createChannel();
            } catch (Exception e) {
                logger.error("Failed to creat Rabbit channel", e);
                throw new RabbitException("Failed to creat Rabbit channel", e);
            }
        }
        return channel;
    }

    @Override
    public boolean put(T value) throws Exception {
        try {
            getChannel().basicPublish(exchange
                    , routingKeyGenerator.makeKey(value)
                    , false
                    , false
                    , messageProperties
                    , converter.apply(value));
            hasOutput = true;
        } catch (Exception e) {
            logger.error(e);
            throw new RabbitException("Failed to publish a message", e);
        }
        return true;
    }

    @Override
    public void setEOF() {
        if (channel != null) {
            try {
                channel.close();
            } catch (Exception e) {
                logger.error("Failed to close Rabbit channel", e);
            }
        }
        if (connection != null && ownConnection) {
            try {
                connection.close();
            } catch (Exception e) {
                logger.error("Failed to close Rabbit connection", e);
            }
        }
    }

    @Override
    public void close() throws Exception {
        setEOF();
    }

    @Override
    public boolean hasOutput() {
        return hasOutput;
    }
}
