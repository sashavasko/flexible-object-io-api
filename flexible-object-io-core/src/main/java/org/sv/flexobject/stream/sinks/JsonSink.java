package org.sv.flexobject.stream.sinks;


import org.sv.flexobject.Streamable;

import java.io.OutputStream;

public class JsonSink extends OutputStreamSink<Streamable>  {

    public JsonSink() {
        super(System.out);
    }

    public JsonSink(OutputStream os) {
        super(os);
    }

    @Override
    protected byte[] convert(Streamable value) {
        try {
            return value.toJsonBytes();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception ee){
            throw new RuntimeException(ee);
        }
    }

    @Override
    public void close() throws Exception {
        if (outputStream() != System.out)
            super.close();
    }
}
