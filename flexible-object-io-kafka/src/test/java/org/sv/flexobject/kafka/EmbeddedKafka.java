package org.sv.flexobject.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;
import org.sv.flexobject.connections.ConnectionManager;
import org.sv.flexobject.connections.PropertiesProvider;

import java.util.Properties;

public class EmbeddedKafka {
    private static final EmbeddedKafkaBroker embeddedKafkaBroker =
        new EmbeddedKafkaKraftBroker(1, 1, "test")
                .brokerListProperty("spring.kafka.bootstrap-servers");
    private  static Properties props = new Properties();

    private static volatile boolean started;

    public static void configureConnectionManager(String connectionName){
        PropertiesProvider embeddedProperties = (connectionName1, deploymentLevel, environment) -> {
            getBroker();
            return connectionName1.equals(connectionName1) ? props : null;
        };
        ConnectionManager.getInstance().registerPropertiesProvider(embeddedProperties);
        ConnectionManager.getInstance().registerProvider(new KafkaProvider());
    }

    public static AdminClient getAdminClient(){
        getBroker();
        return AdminClient.create(props);
    }

    public static EmbeddedKafkaBroker getBroker() {
        if (!started) {
            synchronized (EmbeddedKafkaBroker.class) {
                if (!started) {
                    try {
                        embeddedKafkaBroker.afterPropertiesSet();
                        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafkaBroker.getBrokersAsString());
                        props.put("key.deserializer", ByteArrayDeserializer.class.getName());
                        props.put("value.deserializer", ByteArrayDeserializer.class.getName());
                        props.put("key.serializer", ByteArraySerializer.class.getName());
                        props.put("value.serializer", ByteArraySerializer.class.getName());

                    } catch (Exception e) {
                        throw new KafkaException("Embedded broker failed to start", e);
                    }
                    started = true;
                }
            }
        }
        return embeddedKafkaBroker;
    }
}
