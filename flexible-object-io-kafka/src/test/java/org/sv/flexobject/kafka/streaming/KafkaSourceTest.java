package org.sv.flexobject.kafka.streaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

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
        Mockito.when(mockRecord.value()).thenReturn("foo", "bar");
        assertEquals("foo", source.get());
        assertEquals("bar", source.get());
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
}