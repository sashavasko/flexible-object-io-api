package org.sv.flexobject.io.consumers;

import org.sv.flexobject.OutAdapter;
import org.sv.flexobject.Savable;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.adapter.GenericOutAdapter;
import org.sv.flexobject.io.CloseableConsumer;
import org.sv.flexobject.io.Writer;
import org.sv.flexobject.schema.Schema;
import org.sv.flexobject.stream.Sink;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

public class AdapterConsumer extends CloseableConsumer {

    OutAdapter adapter;
    Writer writer;
    long recordsConsumed = 0;

    public AdapterConsumer(Sink sink, Class<? extends GenericOutAdapter> adapterClass, Writer writer) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        this.adapter = GenericOutAdapter.build(adapterClass, sink);
        this.writer = writer;
    }

    public AdapterConsumer(OutAdapter adapter, Writer writer) {
        this.writer = writer;
        this.adapter = adapter;
    }

    public AdapterConsumer(Sink sink, Class<? extends GenericOutAdapter> adapterClass) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        this(sink, adapterClass, null);
    }

    public AdapterConsumer(OutAdapter adapter) {
        this(adapter, null);
    }

    public boolean consume (Savable datum) {
        if (writer == null && datum instanceof Streamable)
            writer = Schema.getRegisteredSchema(datum.getClass()).getWriter();

        try {
            boolean saved = writer == null
                    ? datum.save(adapter)
                    : writer.convert(datum, adapter);

            if (saved) {
                recordsConsumed++;
                return true;
            }

            if (!adapter.getErrors().isEmpty()){
                setException(adapter.getErrors().get(0), datum);
                adapter.clearErrors();
            }
        }catch (Exception e){
            setException(e, datum);
        }
        return false;
    }

    @Override
    public void close() throws Exception {
        if (adapter instanceof AutoCloseable) {
            ((AutoCloseable) adapter).close();
        }
        super.close();
    }

    @Override
    public List<Exception> getAllErrors() {
        return adapter.getErrors();
    }

    public long getRecordsConsumed() {
        return recordsConsumed;
    }
}
