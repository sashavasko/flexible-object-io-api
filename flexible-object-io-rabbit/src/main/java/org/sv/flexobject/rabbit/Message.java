package org.sv.flexobject.rabbit;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;

public class Message {
    String consumerTag;
    Envelope envelope;
    AMQP.BasicProperties properties;
    byte[] body;

    public Message(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
        this.consumerTag = consumerTag;
        this.envelope = envelope;
        this.properties = properties;
        this.body = body;
    }

    public Envelope getEnvelope() {
        return envelope;
    }

    public AMQP.BasicProperties getProperties() {
        return properties;
    }

    public byte[] getBody() {
        return body;
    }
}
