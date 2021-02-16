package org.sv.flexobject.kafka.streaming;

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
import org.mockito.runners.MockitoJUnitRunner;

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

    @Before
    public void setUp() throws Exception {
        Mockito.when(mockPreparedRecord.getRecord()).thenReturn(mockRecord);
        Mockito.when(mockPreparedRecord.getCallback()).thenReturn(mockCallback);
        sink = new KafkaSink<Long, String>(mockProducer, mockRecordFactory, "test");
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

}