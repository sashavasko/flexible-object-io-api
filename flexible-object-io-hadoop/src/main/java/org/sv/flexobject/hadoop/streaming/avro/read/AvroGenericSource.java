package org.sv.flexobject.hadoop.streaming.avro.read;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.conf.Configured;
import org.sv.flexobject.stream.Source;

import java.io.IOException;
import java.util.stream.Stream;

public class AvroGenericSource<A,T> extends Configured implements Source<T>, AutoCloseable {

    InputBuilder builder;
    DataFileReader<A> reader;

    @Override
    public <O extends T> O  get() throws Exception {
        return unwrap(getReader().next());
    }

    protected <O extends T> O unwrap(A value) {
        return (O)value;
    }

    protected DataFileReader<A> getReader() throws IOException {
        if (reader == null){
            reader = builder().build();
        }
        return reader;
    }

    public static class ThreadSpecificGenericReader extends GenericDatumReader {
        public ThreadSpecificGenericReader() {
            super(new GenericData());
        }
    }

    protected DatumReader<A> buildSourceDatumReader(){
        return new ThreadSpecificGenericReader();
    }

    public InputBuilder builder() {
        if (builder == null){
            builder = new InputBuilder<InputBuilder, A>(){
                @Override
                protected DatumReader<A> buildDatumReader() {
                    return buildSourceDatumReader();
                }
            };
        }

        return builder;
    }

    @Override
    public boolean isEOF() {
        try {
            return !getReader().hasNext();
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public void close() throws Exception {
        if (reader != null){
            reader.close();
            reader = null;
        }
    }

    @Override
    public Stream<T> stream() {
        return null;
    }
}
