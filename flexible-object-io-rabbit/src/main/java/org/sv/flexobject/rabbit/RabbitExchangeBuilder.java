package org.sv.flexobject.rabbit;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class RabbitExchangeBuilder {
    public  static final Logger logger = LogManager.getLogger(RabbitExchangeBuilder.class);

    Connection rabbitConnection;
    String exchangeName;
    BuiltinExchangeType exchangeType = BuiltinExchangeType.FANOUT;

    public class QueueSpec {
        String name;
        boolean durable = false;
        boolean exclusive = false;
        boolean autoDelete = false;
        Map<String, Object> arguments = null;

        String routingKey;

        public QueueSpec routingKey(String routingKey) {
            this.routingKey = routingKey;
            return this;
        }

        public QueueSpec(String name) {
            this.name = name;
        }

        public void durable() {
            this.durable = true;
        }

        public void exclusive() {
            this.exclusive = true;
        }

        public void autoDelete() {
            this.autoDelete = true;
        }

        public void arguments(Map<String, Object> arguments) {
            this.arguments = arguments;
        }

        public QueueSpec declare(Channel channel) throws IOException {
            logger.info("Declaring queue " + name);
            channel.queueDeclare(name, durable, exclusive, autoDelete, arguments);
            return this;
        }
        public QueueSpec bind(Channel channel) throws IOException {
            logger.info("Binding queue " + name + " to " + exchangeName);
            channel.queueBind(name, exchangeName, routingKey);
            return this;
        }
    }

    List<QueueSpec> queues = new ArrayList<>();

    private RabbitExchangeBuilder(Connection rabbitConnection) {
        this.rabbitConnection = rabbitConnection;
    }

    public static RabbitExchangeBuilder forConnection(Connection rabbitConnection){
        return new RabbitExchangeBuilder(rabbitConnection);
    }

    public RabbitExchangeBuilder name(String exchangeName){
        this.exchangeName = exchangeName;
        return this;
    }

    public RabbitExchangeBuilder type(BuiltinExchangeType type){
        this.exchangeType = type;
        return this;
    }

    public RabbitExchangeBuilder addQueue(String queueName){
        queues.add(new QueueSpec(queueName));
        return this;
    }

    public RabbitExchangeBuilder durable(){
        queues.get(queues.size()-1).durable();
        return this;
    }

    public RabbitExchangeBuilder exclusive(){
        queues.get(queues.size()-1).exclusive();
        return this;
    }

    public RabbitExchangeBuilder autoDelete(){
        queues.get(queues.size()-1).autoDelete();
        return this;
    }

    public RabbitExchangeBuilder arguments(Map<String, Object> arguments){
        queues.get(queues.size()-1).arguments(arguments);
        return this;
    }

    public RabbitExchangeBuilder forKey(String routingKey){
        queues.get(queues.size()-1).routingKey(routingKey);
        return this;
    }

    public void build() throws IOException {
        try (Channel channel = rabbitConnection.createChannel()){
            logger.info("Declaring exchange " + exchangeName);
            channel.exchangeDeclare(exchangeName, exchangeType);
            for (QueueSpec queue : queues) {
                queue.declare(channel);
                queue.bind(channel);
            }
        } catch (TimeoutException e) {
            throw new RabbitException("Failed to build exchange " + exchangeName);
        }
    }
}
