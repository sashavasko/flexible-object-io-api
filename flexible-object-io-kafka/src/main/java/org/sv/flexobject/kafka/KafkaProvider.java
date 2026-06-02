package org.sv.flexobject.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sv.flexobject.connections.ConnectionProvider;

import java.util.Arrays;
import java.util.Properties;

public class KafkaProvider implements ConnectionProvider {

    public static final Logger logger = LogManager.getLogger(KafkaProvider.class);

    void logKafkaProps(Properties props) {
        logger.debug("### Kafka props : {}", props.toString());
    }

    @Override
    public AutoCloseable getConnection(String name, Class<? extends AutoCloseable> connectionType, Properties connectionProperties, Object secret) throws Exception {
        if (connectionType == KafkaProducer.class){
            logKafkaProps(connectionProperties);
            // This will throw KafkaException on failure:
            //noinspection RawUseOfParameterized
            KafkaProducer producer = new KafkaProducer(connectionProperties);
            logger.info("Ready to publish to Kafka broker using {}", connectionProperties.get("bootstrap.servers"));
            return producer;
        }
        return getConnection(name, connectionProperties, secret);
    }

    @Override
    public AutoCloseable getConnection(String name, Properties connectionProperties, Object secret) {
        logKafkaProps(connectionProperties);
        // This will throw KafkaException on failure:
        //noinspection RawUseOfParameterized
        KafkaConsumer consumer = new KafkaConsumer(connectionProperties);
        logger.info("Connected to Kafka broker using {}", connectionProperties.get("bootstrap.servers"));
        return consumer;
    }

    @Override
    public Iterable<Class<? extends AutoCloseable>> listConnectionTypes() {
        return Arrays.asList(KafkaConsumer.class, KafkaProducer.class);
    }
}
