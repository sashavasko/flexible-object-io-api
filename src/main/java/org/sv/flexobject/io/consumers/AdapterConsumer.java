package org.sv.flexobject.io.consumers;

import org.sv.flexobject.OutAdapter;
import org.sv.flexobject.Savable;
import org.sv.flexobject.adapter.GenericOutAdapter;
import org.sv.flexobject.io.CloseableConsumer;
import org.sv.flexobject.io.Writer;
import org.sv.flexobject.stream.Sink;

import java.lang.reflect.InvocationTargetException;

public class AdapterConsumer implements CloseableConsumer {

    OutAdapter adapter;
    Writer writer;
    long recordsConsumed = 0;

    public AdapterConsumer(Sink sink, Class<? extends GenericOutAdapter> clazz, Writer writer) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        this.adapter = GenericOutAdapter.build(clazz, sink);
        this.writer = writer;
    }

    public AdapterConsumer(OutAdapter adapter, Writer writer) {
        this.writer = writer;
        this.adapter = adapter;
    }

    public boolean consume (Savable datum) throws Exception {
        if (writer == null){
            if (datum.save(adapter))
                recordsConsumed++;
        }else if (writer.convert(datum, adapter))
            recordsConsumed++;

        return adapter.saveIfYouShould();
    }

    @Override
    public void close() throws Exception {
        if (adapter instanceof AutoCloseable) {
                ((AutoCloseable) adapter).close();
        }
    }

    public long getRecordsConsumed() {
        return recordsConsumed;
    }
}
