package org.sv.flexobject.kafka;

import org.apache.kafka.clients.producer.Callback;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

public class RecordFactory<K,T> {
    Function<T, K> keyExtractor;
    Consumer<RecordDetails<T>> successHandler = null;
    BiConsumer<RecordDetails<T>,Exception> errorHandler = null;

    public RecordFactory(Function<T, K> keyExtractor) {
        this.keyExtractor = keyExtractor;
    }

    public RecordFactory(Function<T, K> keyExtractor, Consumer<RecordDetails<T>> successHandler, BiConsumer<RecordDetails<T>, Exception> errorHandler) {
        this.keyExtractor = keyExtractor;
        this.successHandler = successHandler;
        this.errorHandler = errorHandler;
    }

    public PreparedRecord<K,T> get(String topic, T value){
        RecordDetails<T> details = new RecordDetails<>(topic, value);
        Callback callback = new CallbackWithDetails<>(details, successHandler, errorHandler);
        return new PreparedRecord<>(topic, keyExtractor.apply(value), value, callback);
    }
}
