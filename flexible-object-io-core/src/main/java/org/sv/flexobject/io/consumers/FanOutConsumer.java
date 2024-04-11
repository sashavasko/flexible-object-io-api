package org.sv.flexobject.io.consumers;


import org.sv.flexobject.Savable;
import org.sv.flexobject.io.CloseableConsumer;
import org.sv.flexobject.io.Consumer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class FanOutConsumer extends CloseableConsumer {

    List<Consumer> consumers = new ArrayList<>();
    long recordsConsumed = 0;
    long consumerFailures = 0;
    long recordFailures = 0;

    public FanOutConsumer(Collection<Consumer> consumers) {
        this.consumers.addAll(consumers);
    }

    public FanOutConsumer(Consumer ... consumers) {
        for (Consumer consumer : consumers)
            this.consumers.add(consumer);
    }

    @Override
    public boolean consume(Savable datum) {
        Boolean succees = true;

        for(Consumer consumer : consumers){
            if (!consumer.consume(datum)) {
                succees = false;
                consumerFailures++;
            }
        }
        recordsConsumed++;
        if (!succees)
            recordFailures++;

        return succees;
    }

    @Override
    public long getRecordsConsumed() {
        return recordsConsumed;
    }

    public long getConsumerFailures() {
        return consumerFailures;
    }

    public long getRecordFailures() {
        return recordFailures;
    }

    @Override
    public void close() throws Exception {
        for (Consumer consumer : consumers) {
            try {
                consumer.cleanup();
            }catch (Exception e){
            }

        }
        super.close();
    }

    @Override
    public List<Exception> getAllErrors() {
        List<Exception> exceptions = new ArrayList<>();
        for (Consumer consumer : consumers) {
            exceptions.addAll(consumer.getAllErrors());
        }
        return exceptions;
    }
}
