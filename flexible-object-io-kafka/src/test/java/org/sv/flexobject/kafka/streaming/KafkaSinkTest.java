package org.sv.flexobject.kafka.streaming;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.sv.flexobject.connections.ConnectionManager;
import org.sv.flexobject.kafka.EmbeddedKafka;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class KafkaSinkTest {

    @Mock
    KafkaProducer mockProducer;

    @Mock
    KafkaTestData mockTestData;

    @Mock
    ProducerRecord mockRecord;

    @Mock
    Callback mockCallback;

    @Mock
    Future<RecordMetadata> mockFuture;

    @Spy
    KafkaSink mockSink;

    public static final String TEST_KAFKA_CONNECTION = "test-connection";

    @BeforeEach
    public void setUp() throws Exception {
        when(mockSink.getKafkaProducer()).thenReturn(Optional.of(mockProducer));
        when(mockSink.getTopic()).thenReturn("test");

        EmbeddedKafka.configureConnectionManager(TEST_KAFKA_CONNECTION);
    }

    @Test
    public void put() throws Exception {
        when(mockSink.makeKafkaRecord(mockTestData)).thenReturn(mockRecord);
        when(mockSink.makeKafkaCallback(mockTestData)).thenReturn(mockCallback);
        when(mockProducer.send(mockRecord, mockCallback)).thenReturn(mockFuture);

        boolean result = mockSink.put(mockTestData);
        assertTrue(result);

        Mockito.verify(mockProducer).send(mockRecord, mockCallback);
    }

    @Test
    public void setEOF() {
        mockSink.setEOF();
        Mockito.verify(mockProducer).flush();
    }

    @Test
    public void hasOutput() {
        assertFalse(mockSink.hasOutput());
    }

    public long countMessages(String topic) throws Exception {
        try (KafkaConsumer consumer = (KafkaConsumer) ConnectionManager.getConnection(KafkaConsumer.class, TEST_KAFKA_CONNECTION)) {
            // 1. Get partitions for the topic
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
            List<TopicPartition> partitions = partitionInfos.stream()
                    .map(pi -> new TopicPartition(pi.topic(), pi.partition()))
                    .collect(Collectors.toList());

            // 2. Fetch beginning and end offsets
            Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(partitions);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);

            // 3. Sum the differences
            long totalMessages = partitions.stream()
                    .mapToLong(tp -> endOffsets.get(tp) - beginningOffsets.get(tp))
                    .sum();

            System.out.println("Total messages: " + totalMessages);
            return totalMessages;
        }
    }

    @Test
    public void embeddedKafka() throws Exception {

        long startingCount = countMessages("test");
        KafkaTestData data1 = KafkaTestData.of(123l, "foo");
        KafkaTestData data2 = KafkaTestData.of(123l, "bar");
        try(KafkaSink<KafkaTestData> sink = KafkaSink.<KafkaTestData>builder()
                .forConnection(TEST_KAFKA_CONNECTION)
                .topic("test")
                .build()){
            sink.put(data1);
            sink.put(data2);
            sink.setEOF();
            System.out.println("Done publishing");
            assertEquals(0, sink.getFailedAcks().size());
        }

        assertEquals(startingCount+2, countMessages("test"));
    }
}