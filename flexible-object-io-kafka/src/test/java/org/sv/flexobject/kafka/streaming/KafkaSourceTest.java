package org.sv.flexobject.kafka.streaming;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.sv.flexobject.avro.AvroSerializationStrategy;
import org.sv.flexobject.kafka.EmbeddedKafka;

import java.io.IOException;
import java.time.Duration;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
public class KafkaSourceTest {

    @Mock
    KafkaConsumer mockConsumer;

    @Mock
    ConsumerRecords mockRecords;

    @Mock
    Iterator mockIterator;

    @Mock
    ConsumerRecord mockRecord;


    Duration mockTimeout = Duration.ofSeconds(1234567);

    KafkaSource<KafkaTestData> source;
    KafkaTestData data1 = KafkaTestData.of(1L, "foo");
    KafkaTestData data2 = KafkaTestData.of(2L, "bar");
    public static final String TEST_KAFKA_CONNECTION = "test-connection";

    @BeforeEach
    public void setUp() throws Exception {
        source = KafkaSource.builder()
                .addTopic("foo")
                .forSchema(KafkaTestData.class)
                .useConsumer(mockConsumer)
                .timeoutSeconds(1234567)
                .build();

    }

    @Test
    public void get() throws Exception {
        Mockito.when(mockConsumer.poll(mockTimeout)).thenReturn(mockRecords);
        Mockito.when(mockRecords.iterator()).thenReturn(mockIterator);
        Mockito.when(mockRecords.isEmpty()).thenReturn(false, true);
        Mockito.when(mockIterator.hasNext()).thenReturn(true,true, true, false);
        Mockito.when(mockIterator.next()).thenReturn(mockRecord);

        Mockito.when(mockRecord.value()).thenReturn(data1.getKafkaValue(AvroSerializationStrategy.AVRO), data2.getKafkaValue(AvroSerializationStrategy.AVRO));

        assertEquals(data1, source.get());
        assertEquals(data2, source.get());
        assertNull(source.get());
    }

    @Test
    public void isEOF() {
        Mockito.when(mockConsumer.poll(mockTimeout)).thenReturn(mockRecords);
        Mockito.when(mockRecords.iterator()).thenReturn(mockIterator);
        Mockito.when(mockRecords.isEmpty()).thenReturn(false, true);
        Mockito.when(mockIterator.hasNext()).thenReturn(true, false);

        assertFalse(source.isEOF());
        assertTrue(source.isEOF());
    }

    @Test
    public void ack() {
        source.ack();

        Mockito.verify(mockConsumer).commitSync();
    }

    @Test
    public void close() throws Exception {
        source.close();

        Mockito.verify(mockConsumer).close();
    }

    @Test
    public void setEOF() {
        source.setEOF();

        Mockito.verify(mockConsumer).close();
    }

    @Test
    void roundTrip() throws Exception {
        EmbeddedKafka.configureConnectionManager(TEST_KAFKA_CONNECTION);
        try(SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient()){
            RegistryAwareAvroSerializationStrategy serializationStrategy = RegistryAwareAvroSerializationStrategy.builder()
                    .autoRegister(schemaRegistryClient, EmbeddedKafka.TEST_TOPIC)
                    .forSchema(KafkaTestData.class)
                    .build();
            try(KafkaSink<KafkaTestData> sink = KafkaSink.<KafkaTestData>builder()
                    .forConnection(TEST_KAFKA_CONNECTION)
                    .serializeWith(serializationStrategy)
                    .topic(EmbeddedKafka.TEST_TOPIC)
                    .build()){
                sink.put(data1);
                sink.put(data2);
                sink.setEOF();
            }

            KafkaTestData dataOut1;
            KafkaTestData dataOut2;
            try(KafkaSource<KafkaTestData> source  = KafkaSource.<KafkaTestData>builder()
                    .forConnection(TEST_KAFKA_CONNECTION)
                    .withSchemaRegistry(schemaRegistryClient)
                    .addTopic(EmbeddedKafka.TEST_TOPIC)
                    .build()){
                dataOut1 = source.get();
                dataOut2 = source.get();
                source.ack();
            }

            assertEquals(data1, dataOut1);
            assertEquals(data2, dataOut2);
        }

    }
}