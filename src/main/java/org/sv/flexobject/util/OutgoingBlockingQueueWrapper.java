package org.sv.flexobject.util;

import org.sv.flexobject.stream.Sink;
import org.sv.flexobject.stream.Source;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class OutgoingBlockingQueueWrapper<T> extends NotReallyBlockingQueueWrapper<T> implements Sink<T>, Source<T> {
    long timeout;
    TimeUnit timeoutUnit;

    public OutgoingBlockingQueueWrapper(BlockingQueue<T> queue) {
        this(queue, -1, TimeUnit.SECONDS);
    }

    public OutgoingBlockingQueueWrapper(BlockingQueue<T> queue, long timeout, TimeUnit unit) {
        super(queue);
        this.timeout = timeout;
        this.timeoutUnit = unit;
    }

    public boolean put(T value) throws InterruptedException, TimeoutException {
        if (value == null)
            return false;

        timedPut(value);
        return true;
    }

    public T get() throws Exception {
        if (timeout < 0){
            return timedTake();
        } else if (timeout == 0) {
            return queue.poll();
        } else
            return queue.poll(timeout, timeoutUnit);
    }

}
