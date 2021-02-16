package org.sv.flexobject.kafka.streaming;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sv.flexobject.kafka.PreparedRecord;
import org.sv.flexobject.kafka.RecordDetails;
import org.sv.flexobject.kafka.RecordFactory;
import org.sv.flexobject.stream.Sink;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;

public class KafkaSink<K, T> implements Sink<T> {
    Logger logger = LogManager.getLogger(KafkaSink.class);
    String topic;
    RecordFactory<K, T> recordFactory;
    KafkaProducer<K, T> kafkaProducer;
    Map<T,Exception> failedAcks = new HashMap<>();

    public void onSuccess(RecordDetails details) {
        logger.debug("kafka ack to the message " + details.getValue());
    }

    public void onFailure(RecordDetails<T> details, Exception e){
        failedAcks.put(details.getValue(), e);
        logger.error("kafka ack failed to the message " + details.getValue(), e);
    }

    public KafkaSink(KafkaProducer producer, RecordFactory<K, T> recordFactory, String topic) {
        this.recordFactory = recordFactory;
        this.topic = topic;
        this.kafkaProducer = producer;
    }

    public KafkaSink(Properties properties, Function<T, K> keyExtractor, String topic) {
        this(new KafkaProducer<K, T>(properties), null,
                topic);
        setRecordFactory(new RecordFactory<K,T>(keyExtractor,this::onSuccess, this::onFailure));
    }

    public void setRecordFactory(RecordFactory<K, T> recordFactory) {
        this.recordFactory = recordFactory;
    }

    @Override
    public boolean put(T value) throws Exception {
        PreparedRecord<K, T> record = recordFactory.get(topic, value);
        kafkaProducer.send(record.getRecord(), record.getCallback());
        return true;
    }

    @Override
    public void setEOF() {
        kafkaProducer.flush();
    }

    @Override
    public boolean hasOutput() {
        return false;
    }

    public Map<T, Exception> getFailedAcks() {
        return failedAcks;
    }
}
