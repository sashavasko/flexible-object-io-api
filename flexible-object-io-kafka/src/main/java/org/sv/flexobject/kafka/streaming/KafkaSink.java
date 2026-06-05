package org.sv.flexobject.kafka.streaming;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sv.flexobject.connections.ConnectionManager;
import org.sv.flexobject.kafka.CallbackWithDetails;
import org.sv.flexobject.kafka.KafkaStreamable;
import org.sv.flexobject.kafka.RecordDetails;
import org.sv.flexobject.stream.Sink;
import org.sv.flexobject.util.InstanceFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class KafkaSink<T extends KafkaStreamable> implements Sink<T>, AutoCloseable {
    Logger logger = LogManager.getLogger(KafkaSink.class);
    String topic;
    KafkaProducer<byte[], byte[]> kafkaProducer;
    Map<T,Exception> failedAcks = new HashMap<>();

    public void onSuccess(RecordDetails<T> details) {
        logger.debug("Kafka ack to the message {}", details.getValue());
    }

    public void onFailure(RecordDetails<T> details, Exception e){
        failedAcks.put(details.getValue(), e);
        logger.error("Kafka ack failed to the message {}", details.getValue(), e);
    }

    public KafkaSink() {
    }

    public static class Builder<ST extends KafkaStreamable>{

        KafkaSink<ST> sink = InstanceFactory.get(KafkaSink.class);

        public Builder<ST> topic(String topic){
            sink.topic = topic;
            return this;
        }

        public Builder<ST> forConnection(String name) throws Exception {
            sink.kafkaProducer = (KafkaProducer<byte[], byte[]>) ConnectionManager.getConnection(KafkaProducer.class, name);

            return this;
        }

        protected Builder<ST> useProducer(KafkaProducer<byte[], byte[]> producer){
            sink.kafkaProducer = producer;
            return this;
        }

        public KafkaSink<ST> build(){
            if (StringUtils.isBlank(sink.topic)){
                throw new KafkaException("Topic is empty");
            }
            if (sink.kafkaProducer == null){
                throw new KafkaException("Kafka Connection is not set");
            }
            return sink;
        }
    }

    public static <SP extends KafkaStreamable> Builder<SP> builder(){
        return new Builder<>();
    }

    @Override
    public boolean put(T value) throws Exception {
        return getKafkaProducer()
                .map(p->p.send(makeKafkaRecord(value), makeKafkaCallback(value))!= null)
                .orElse(false);
    }

    @Override
    public void setEOF() {
        getKafkaProducer().ifPresent(KafkaProducer::flush);
    }

    @Override
    public boolean hasOutput() {
        return false;
    }

    public Map<T, Exception> getFailedAcks() {
        return failedAcks;
    }

    @Override
    public void close() throws Exception {
        getKafkaProducer().ifPresent(p->p.close(Duration.ofMillis(10000)));
        kafkaProducer = null;
    }

    public Optional<KafkaProducer<byte[], byte[]>> getKafkaProducer(){
        return Optional.ofNullable(kafkaProducer);
    }

    public String getTopic() {
        return topic;
    }

    protected ProducerRecord<byte[], byte[]> makeKafkaRecord(T value){
        return new ProducerRecord<>(getTopic(), value.getKafkaKey(), value.getKafkaValue());
    }

    protected Callback makeKafkaCallback(T value){
        return new CallbackWithDetails<T>(getTopic(), value, this::onSuccess, this::onFailure);
    }
}
