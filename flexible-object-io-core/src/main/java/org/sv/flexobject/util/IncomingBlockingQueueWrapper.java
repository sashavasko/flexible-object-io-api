package org.sv.flexobject.util;

import org.sv.flexobject.stream.Sink;
import org.sv.flexobject.stream.Source;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class IncomingBlockingQueueWrapper<T> extends NotReallyBlockingQueueWrapper<T> implements Sink<T>, Source<T> {
    public static int MAX_WAIT_TIMEOUT_SECONDS = 60*5;
    long timeout;
    TimeUnit timeoutUnit;

    public IncomingBlockingQueueWrapper(BlockingQueue<T> queue) {
        this(queue, -1, TimeUnit.SECONDS);
    }

    public IncomingBlockingQueueWrapper(BlockingQueue<T> queue, long timeout, TimeUnit unit) {
        super(queue);
        this.timeout = timeout;
        this.timeoutUnit = unit;
    }

    public boolean put(T value) throws InterruptedException, TimeoutException {
        if (value == null)
            return false;
        if (timeout < 0){
            timedPut(value);
            return true;
        } else if (timeout == 0) {
            return queue.offer(value);
        } else
            return queue.offer(value, timeout, timeoutUnit);
    }

    public T get() throws Exception {
        T value;
        while((value = queue.poll()) == null) {
//            logger.info("queue size is " + queue.size() + " and EOF is " + EOF);
            if (EOF) {
                return null;
            }

            Thread.sleep(10);
        }
        return value;
    }
}
