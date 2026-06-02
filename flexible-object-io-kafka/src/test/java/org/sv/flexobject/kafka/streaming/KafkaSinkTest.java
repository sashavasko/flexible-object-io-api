package org.sv.flexobject.kafka.streaming;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;
import org.sv.flexobject.StreamableImpl;
import org.sv.flexobject.connections.ConnectionManager;
import org.sv.flexobject.kafka.EmbeddedKafka;
import org.sv.flexobject.kafka.PreparedRecord;
import org.sv.flexobject.kafka.RecordFactory;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

@RunWith(MockitoJUnitRunner.class)
public class KafkaSinkTest {

    @Mock
    KafkaProducer mockProducer;

    @Mock
    RecordFactory mockRecordFactory;

    @Mock
    ProducerRecord mockRecord;

    @Mock
    PreparedRecord mockPreparedRecord;

    @Mock
    Callback mockCallback;

    KafkaSink<Long, String> sink;

    public static final String TEST_KAFKA_CONNECTION = "test-connection";

    @Before
    public void setUp() throws Exception {
        Mockito.when(mockPreparedRecord.getRecord()).thenReturn(mockRecord);
        Mockito.when(mockPreparedRecord.getCallback()).thenReturn(mockCallback);
        sink = KafkaSink.builder().topic("test").useProducer(mockProducer).recordFactory(mockRecordFactory).build();
        EmbeddedKafka.configureConnectionManager(TEST_KAFKA_CONNECTION);
    }

    @Test
    public void put() throws Exception {
        Mockito.when(mockRecordFactory.get("test", "1234567"))
                .thenReturn(mockPreparedRecord);
//        Mockito.when(mockProducer.send(mockRecord, mockCallback)).
        boolean result = sink.put("1234567");

        assertTrue(result);
        Mockito.verify(mockProducer).send(mockRecord, mockCallback);
    }

    @Test
    public void setEOF() {
        sink.setEOF();
        Mockito.verify(mockProducer).flush();
    }

    @Test
    public void hasOutput() {
        assertFalse(sink.hasOutput());
    }


    public EmbeddedKafkaKraftBroker embeddedKafkaBroker(int count, int partitions, String[] topics) {
        EmbeddedKafkaKraftBroker embeddedKafkaBroker = new EmbeddedKafkaKraftBroker(count, partitions, topics);
        return embeddedKafkaBroker;
    }

    public static class KafkaTestData extends StreamableImpl {
        Long key;
        String value;

        public static KafkaTestData of(long key, String value) {
            KafkaTestData kafkaTestData = new KafkaTestData();
            kafkaTestData.key = key;
            kafkaTestData.value = value;
            return kafkaTestData;
        }

        public static Long getKey(KafkaTestData data) {
            return data.key;
        }
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
        Function<KafkaTestData, Long> keyExtractor = KafkaTestData::getKey;

        long startingCount = countMessages("test");
        KafkaTestData data1 = KafkaTestData.of(123l, "foo");
        KafkaTestData data2 = KafkaTestData.of(123l, "bar");
        try(KafkaSink sink = KafkaSink.builder()
                .forConnection(TEST_KAFKA_CONNECTION)
                .topic("test")
                .keyExtractor(keyExtractor)
                .build()){
            sink.put(data1);
            sink.put(data2);
        }
//        assertEquals(startingCount+2, countMessages("test"));
    }
}