package org.sv.flexobject.rabbit.streaming;

import com.rabbitmq.client.Channel;

import java.io.IOException;

public abstract class MessageListenerFinalSink<SELF> implements MessageListenerSink {

    Channel channel;
    private volatile String _consumerTag;

    @Override
    public void bind(Channel channel, String queueName, String consumerTag) throws IOException {
        this.channel = channel;
        channel.basicConsume(queueName, false, consumerTag, this);
    }

    public Channel getChannel() {
        return channel;
    }

    @Override
    public void storeConsumerTag(String consumerTag) {
        _consumerTag = consumerTag;
    }

    @Override
    public String getLastConsumerTag() {
        return _consumerTag;
    }
}
