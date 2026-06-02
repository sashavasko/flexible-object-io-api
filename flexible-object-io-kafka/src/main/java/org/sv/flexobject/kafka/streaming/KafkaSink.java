package org.sv.flexobject.kafka.streaming;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.KafkaException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.sv.flexobject.connections.ConnectionManager;
import org.sv.flexobject.kafka.PreparedRecord;
import org.sv.flexobject.kafka.RecordDetails;
import org.sv.flexobject.kafka.RecordFactory;
import org.sv.flexobject.stream.Sink;
import org.sv.flexobject.util.AutoCloseables;
import org.sv.flexobject.util.InstanceFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

public class KafkaSink<K, T> implements Sink<T>, AutoCloseable {
    Logger logger = LogManager.getLogger(KafkaSink.class);
    String topic;
    RecordFactory<K, T> recordFactory;
    KafkaProducer<K, T> kafkaProducer;
    Map<T,Exception> failedAcks = new HashMap<>();

    public void onSuccess(RecordDetails details) {
        logger.debug("Kafka ack to the message " + details.getValue());
    }

    public void onFailure(RecordDetails<T> details, Exception e){
        failedAcks.put(details.getValue(), e);
        logger.error("Kafka ack failed to the message " + details.getValue(), e);
    }

    public KafkaSink() {
    }

    public static class Builder{

        KafkaSink sink = InstanceFactory.get(KafkaSink.class);

        public Builder topic(String topic){
            sink.topic = topic;
            return this;
        }

        public Builder recordFactory(RecordFactory factory){
            sink.recordFactory = factory;
            return this;
        }

        public Builder keyExtractor(Function keyExtractor){
            Consumer<RecordDetails> consumer = sink::onSuccess;
            BiConsumer<RecordDetails,Exception> errorHandler = sink::onFailure;
            sink.recordFactory = new RecordFactory(keyExtractor,consumer, errorHandler);
            return this;
        }

        public Builder forConnection(String name) throws Exception {
            sink.kafkaProducer = (KafkaProducer) ConnectionManager.getConnection(KafkaProducer.class, name);
            return this;
        }

        protected Builder useProducer(KafkaProducer producer){
            sink.kafkaProducer = producer;
            return this;
        }

        public KafkaSink build(){
            if (StringUtils.isBlank(sink.topic)){
                throw new KafkaException("Topic is empty");
            }
            if (sink.recordFactory == null){
                throw new KafkaException("Key Extractor / Record Factory is not set");
            }
            if (sink.kafkaProducer == null){
                throw new KafkaException("Kafka Connection is not set");
            }
            return sink;
        }
    }

    public static Builder builder(){
        return new Builder();
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

    @Override
    public void close() throws Exception {
        AutoCloseables.close(kafkaProducer);
    }
}
