package org.sv.flexobject.hadoop.streaming.avro.write;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.sv.flexobject.stream.Sink;

import java.io.IOException;

abstract public class AvroGenericSink<A, T> extends Configured implements Sink<T>, AutoCloseable {

    OutputBuilder builder;
    DataFileWriter<A> writer = null;
    boolean hasOutput = false;

    @Override
    public void setConf(Configuration conf) {
        super.setConf(conf);
        builder().withConf(conf);
    }

    protected DatumWriter<A> buildSinkDatumWriter(){
        return new GenericDatumWriter<>();
    }

    public OutputBuilder builder(){
        if (builder == null){
            builder = new OutputBuilder<OutputBuilder, A>(){
                @Override
                protected DatumWriter<A> buildDatumWriter() {
                    return buildSinkDatumWriter();
                }
            };
        }
        return builder;
    }

    protected A wrap(T originalValue){
        return (A) originalValue;
    }

    @Override
    public boolean put(T value) throws Exception {
        getWriter().append(wrap(value));
        hasOutput = true;
        return false;
    }

    private DataFileWriter<A> getWriter() throws IOException {
        if (writer == null){
            writer = builder().build();
        }
        return writer;
    }

    @Override
    public boolean hasOutput() {
        return hasOutput;
    }

    @Override
    synchronized public void close() throws Exception {
        if (writer != null){
            writer.close();
            writer = null;
        }
    }
}
