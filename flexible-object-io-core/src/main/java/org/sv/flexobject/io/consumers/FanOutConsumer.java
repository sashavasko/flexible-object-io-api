package org.sv.flexobject.io.consumers;


import org.sv.flexobject.Savable;
import org.sv.flexobject.io.Consumer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class FanOutConsumer implements Consumer {

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
    public boolean consume(Savable datum) throws Exception {
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
    public void cleanup() throws Exception {
    }
}
