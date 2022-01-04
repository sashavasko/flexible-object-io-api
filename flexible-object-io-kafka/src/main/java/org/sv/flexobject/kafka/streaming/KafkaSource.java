package org.sv.flexobject.kafka.streaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.sv.flexobject.stream.Source;

import java.time.Duration;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class KafkaSource<K, T> implements Source<T>, Iterator<T>, AutoCloseable {
    KafkaConsumer<K, T> kafkaConsumer;
    Duration timeout = Duration.ofSeconds(60);
    ConsumerRecords<K,T> currentRecords = null;
    Iterator<ConsumerRecord<K,T>> currentRecordIterator = null;

    public KafkaSource(KafkaConsumer<K, T> kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
    }

    public KafkaSource(Properties props, List<String> topics, long timeoutSeconds) {
        this(new KafkaConsumer<>(props));
        kafkaConsumer.subscribe(topics);
        if (timeoutSeconds > 0)
            timeout = Duration.ofSeconds(timeoutSeconds);
    }

    public void setTimeout(Duration timeout) {
        this.timeout = timeout;
    }

    protected boolean pollRecords(){
        if (kafkaConsumer == null)
            return false;

        if (currentRecords == null || !currentRecordIterator.hasNext()){
            currentRecords = kafkaConsumer.poll(timeout);
            currentRecordIterator = currentRecords.iterator();
            if (currentRecords.isEmpty())
                return false;
        }
        return currentRecordIterator.hasNext();
    }

    @Override
    public T get() throws Exception {
        return next();
    }

    @Override
    public boolean isEOF() {
        return !pollRecords();
    }

    @Override
    public void ack(){
        kafkaConsumer.commitSync();
    }

    @Override
    public void close() throws Exception {
        kafkaConsumer.close();
        kafkaConsumer = null;
    }

    @Override
    public Stream<T> stream() {
        return StreamSupport.stream(spliterator(), false);
    }

    @Override
    public void setEOF() {
        try {
            close();
        } catch (Exception e) {

        }
    }

    @Override
    public boolean hasNext() {
        return pollRecords();
    }

    @Override
    public T next() {
        return pollRecords() ? currentRecordIterator.next().value() : null;
    }
}
