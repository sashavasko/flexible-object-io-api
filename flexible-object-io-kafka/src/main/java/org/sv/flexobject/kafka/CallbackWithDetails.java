package org.sv.flexobject.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class CallbackWithDetails<T> implements Callback {
    RecordDetails<T> details;
    Consumer<RecordDetails<T>> onSuccess;
    BiConsumer<RecordDetails<T>, Exception> onFailure;

    public CallbackWithDetails(RecordDetails<T> details, Consumer<RecordDetails<T>> onSuccess, BiConsumer<RecordDetails<T>, Exception> onFailure) {
        this.details = details;
        this.onSuccess = onSuccess;
        this.onFailure = onFailure;
    }

    public RecordDetails<T> getDetails() {
        return details;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        details.setMetadata(metadata);
        if (exception != null) {
            if (onFailure != null)
                onFailure.accept(details, exception);
        } else if(onSuccess != null) {
                onSuccess.accept(details);
        }
    }
}
