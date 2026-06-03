package org.sv.flexobject.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.sv.flexobject.Streamable;

public class PreparedRecord<K,T extends Streamable> {
    ProducerRecord<K,String> record;
    Callback callback;

    public PreparedRecord(String topic, K key, T value, Callback callback) {
        this.record = new ProducerRecord<>(topic, key, value.toString());
        this.callback = callback;
    }

    public ProducerRecord getRecord() {
        return record;
    }

    public Callback getCallback() {
        return callback;
    }
}
