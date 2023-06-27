package org.sv.flexobject.rabbit;

import org.sv.flexobject.Streamable;
import org.sv.flexobject.rabbit.domain.PayloadNested;
import org.sv.flexobject.rabbit.domain.PayloadSimple;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Connection;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;
import org.sv.flexobject.rabbit.streaming.BufferingSinkFactory;
import org.sv.flexobject.rabbit.streaming.MessageConsumer;
import org.sv.flexobject.rabbit.streaming.RabbitSink;
import org.sv.flexobject.rabbit.streaming.RoutingKeyGenerator;

import java.util.Arrays;
import java.util.List;

// To run :
// hdpjob -jar build/libs/lib/dt-streaming-examples-1.0.0-SNAPSHOT.jar -drv com.carfax.dt.streaming.examples.rabbit.SimpleRoundtrip
public class ThreadedRoundtrip extends Configured implements Tool {

    Logger logger = Logger.getLogger(ThreadedRoundtrip.class);
    public static final String CONNECTION_NAME = "testRabbit";
    public static final String EXCHANGE_NAME = "testExchange";
    public static final String QUEUE_NAME = "testQueue";
    public static final String ROUTING_KEY = "testRoute";
    public static final String RABBIT_PASSWORD = "guest";
    public static final String RABBIT_USER = "guest";
    // Make sure Rabbit container is running
    // # sudo docker run -d --hostname test-rabbit-host -P --name test-rabbit rabbitmq:3-management
    // and you plug appropriate ports that are bound to rabbit
    // # sudo docker ps | grep 5672
    public static final String RABBIT_HOST = "enthadoopcld02p.d.carfax.us";
    public static final int RABBIT_PORT = 32774;

    List<Streamable> simpleSamples = Arrays.asList(new PayloadSimple(10, "foo"), new PayloadSimple(11, "bar"), new PayloadSimple(12, "The big brown fox eats a lazy frog"));
    List<Streamable> nestedSamples = Arrays.asList(new PayloadNested(new PayloadSimple(111, "I'm deep"), "nested payload"), new PayloadNested(new PayloadSimple(112, "I'm deep too"), "nested payload as well"));

    public void publishSamples(RabbitSink sink) throws Exception {
        PayloadSimple payload = new PayloadSimple(0, "some info");
        logger.info("#### start publishing ");
        while (payload.getIntVal() < 1000) {
            sink.put(payload.inc());
            System.out.println("out " + payload);
            Thread.sleep(1);
        }
        logger.info("#### done publishing. Published " + payload.getIntVal());
    }

    public class ListenerThread implements Runnable{

        BufferingSinkFactory buffer;

        public ListenerThread(BufferingSinkFactory buffer) {
            this.buffer = buffer;
        }

        @Override
        public void run() {
            try {
                System.out.println("*** start listening");
                while (buffer.getBuffer().isEmpty()) {
                    Thread.sleep(1);
                }

                int current = 0;
                while (current < buffer.getBuffer().size()) {
                    List<Streamable> receivedValues = buffer.getBuffer();
                    while (current < receivedValues.size()) {
                        System.out.println("in " + receivedValues.get(current++));
                    }
                    Thread.sleep(1);
                    if (current == buffer.getBuffer().size())
                        Thread.sleep(1000);
                }
                System.out.println("*** done listening. Received " + current);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        RabbitConnectionConf conf = new RabbitConnectionConf(RABBIT_HOST, RABBIT_PORT, RABBIT_USER);

        //        Connection rabbitConnection = (Connection) ConnectionManager.
//                getConnection(Connection.class, CONNECTION_NAME);
        logger.info("Connecting to Rabbit ...");
        try (Connection rabbitConnection = new RabbitConnectionProvider().getConnection(CONNECTION_NAME, conf, RABBIT_PASSWORD)) {
            RabbitExchangeBuilder
                    .forConnection(rabbitConnection)
                    .name(EXCHANGE_NAME)
                    .type(BuiltinExchangeType.FANOUT)
                    .addQueue(QUEUE_NAME).forKey(ROUTING_KEY)
                    .build();

            BufferingSinkFactory buffer = new BufferingSinkFactory(PayloadSimple.class);

            logger.info("Starting consumer...");
            try(MessageConsumer consumer = MessageConsumer.builder()
                    .forConnection(rabbitConnection)
                    .addQueue(QUEUE_NAME)
                    .concurrentConsumers(2)
                    .target(buffer)
                    .build()) {
                Thread listener = new Thread(new ListenerThread(buffer));
                listener.start();

                logger.info("Building the publisher sinks...");
                try (RabbitSink sink = RabbitSink.builder()
                        .forConnection(rabbitConnection)
                        .toExchange(EXCHANGE_NAME)
                        .routeUsing(new RoutingKeyGenerator.ConstantString(ROUTING_KEY))
                        .type(PayloadSimple.class)
                        .build()) {

                    publishSamples(sink);

                    logger.info("Closing publishers ...");
                }
                listener.join();
                logger.info("Closing consumers ...");
            }

            logger.info("Closing connection ...");
        }
        logger.info("Done!");
        return 0;
    }
}
