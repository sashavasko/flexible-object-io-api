package org.sv.flexobject.io.consumers;

import org.sv.flexobject.Savable;
import org.sv.flexobject.io.CloseableConsumer;
import org.sv.flexobject.stream.Sink;

public class SinkConsumer<T extends Savable> implements CloseableConsumer {
    Sink<T> sink;
    long recordsConsumed = 0;

    public SinkConsumer(Sink<T> sink) {
        this.sink = sink;
    }

    @Override
    public void close() throws Exception {
        if (sink instanceof AutoCloseable)
            ((AutoCloseable)sink).close();
        else
            sink.setEOF();
    }

    @Override
    public boolean consume(Savable datum) throws Exception {
        recordsConsumed++;
        return sink.put((T) datum);
    }

    @Override
    public long getRecordsConsumed() {
        return recordsConsumed;
    }
}
