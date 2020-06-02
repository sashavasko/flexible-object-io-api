package org.sv.flexobject.util;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class NotReallyBlockingQueueWrapper<T> {
    public static int MAX_WAIT_TIMEOUT_SECONDS = 60*5;
    protected BlockingQueue<T> queue;
    boolean EOF = false;

    public NotReallyBlockingQueueWrapper() {
    }

    public NotReallyBlockingQueueWrapper(BlockingQueue<T> queue) {
        this.queue = queue;
    }

    protected void timedPut(T value) throws TimeoutException, InterruptedException {
        if (!queue.offer(value, MAX_WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS)){
            throw new TimeoutException("Max allowed time for put() exceeded");
        }
    }

    protected T timedTake() throws InterruptedException, TimeoutException {
        T value = queue.poll(MAX_WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        if (value == null){
            throw new TimeoutException("Max allowed time for get() exceeded");
        }
        return value;
    }


    public void clear() {
        queue.clear();
    }

    public BlockingQueue<T> getQueue(){
        return queue;
    }

    public boolean isEOF() {
        return EOF && queue.isEmpty();
    }

    public void setEOF() {

        this.EOF = true;
    }

    public boolean hasOutput() {
        return !getQueue().isEmpty();
    }
}
