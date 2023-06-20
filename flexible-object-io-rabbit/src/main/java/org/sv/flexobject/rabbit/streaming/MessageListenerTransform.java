package org.sv.flexobject.rabbit.streaming;

import org.apache.commons.lang3.StringUtils;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.rabbit.Message;
import com.rabbitmq.client.Channel;
import org.sv.flexobject.stream.Sink;
import org.sv.flexobject.stream.sinks.TransformSink;
import org.sv.flexobject.util.InstanceFactory;

import java.io.IOException;

public class MessageListenerTransform<SELF> extends TransformSink<SELF, Message, Streamable> implements MessageListenerSink {

    Channel channel;
    private volatile String _consumerTag;
    private Class <? extends Streamable> defaultType;

    public SELF setDefaultType(Class<? extends Streamable> defaultType) {
        this.defaultType = defaultType;
        return (SELF) this;
    }

    public MessageListenerTransform() {
        setTransform(this::streamableJson);
    }

    public MessageListenerTransform(Sink outputSink) {
        super(outputSink);
        setTransform(this::streamableJson);
    }

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

    public Streamable makeInstance(Message message) throws ClassNotFoundException {
        String payloadType = message.getProperties().getType();
        if (StringUtils.isNotBlank(payloadType))
            return (Streamable) InstanceFactory.get(Class.forName(payloadType));
        return InstanceFactory.get(defaultType);
    }
    public Streamable streamableJson(Message message) throws Exception{
        Streamable output = makeInstance(message);
        if (message.getProperties().getContentEncoding() != null)
            output.fromJsonBytes(message.getBody(), message.getProperties().getContentEncoding());
        else
            output.fromJsonBytes(message.getBody());
        return output;
    }
}
