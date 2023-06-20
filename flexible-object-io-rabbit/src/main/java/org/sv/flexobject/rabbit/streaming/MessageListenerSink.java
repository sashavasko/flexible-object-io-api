package org.sv.flexobject.rabbit.streaming;

import org.sv.flexobject.rabbit.Message;
import org.sv.flexobject.rabbit.RabbitException;
import com.rabbitmq.client.*;
import org.sv.flexobject.stream.Sink;

import java.io.IOException;

public interface MessageListenerSink extends Sink<Message>, Consumer {

    void bind(Channel channel, String queueName, String consumerTag) throws IOException;

    Channel getChannel();

    @Override
    default void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        boolean aboutToAck = false;
        try {
            String routingKey = envelope.getRoutingKey();
            String contentType = properties.getContentType();
            long deliveryTag = envelope.getDeliveryTag();
            put(new Message(consumerTag, envelope, properties, body));
            aboutToAck = true;
            getChannel().basicAck(deliveryTag, false);
        } catch (Exception e) {
            if (aboutToAck)
                throw new RabbitException("Failed to ack the message", e);

            if (e instanceof IOException)
                throw (IOException) e;
            throw new IOException("Failed to consume a message", e);
        }
    }

    @Override
    default void handleConsumeOk(String consumerTag) {
        storeConsumerTag (consumerTag);
    }

    void storeConsumerTag(String consumerTag);
    String getLastConsumerTag();
    @Override
    default void handleCancelOk(String consumerTag) {
    }

    @Override
    default void handleCancel(String consumerTag) throws IOException {
    }

    @Override
    default void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
    }

    @Override
    default void handleRecoverOk(String consumerTag) {
    }
}
