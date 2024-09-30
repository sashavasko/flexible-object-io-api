package org.sv.flexobject.hadoop.streaming.avro.read;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.sv.flexobject.Streamable;

import java.io.IOException;

public class GenericInputBuilder extends InputBuilder<GenericInputBuilder, GenericRecord> {

    DatumReader<GenericRecord> reader;

    public GenericInputBuilder() {
    }

    public GenericInputBuilder(DatumReader<GenericRecord> reader) {
        this.reader = reader;
    }

    public static DataFileReader<GenericRecord> forData(byte[] data, Class<? extends Streamable> dataClass) throws IOException {
        GenericInputBuilder builder = new GenericInputBuilder();
        return builder.forInput(data).withSchema(dataClass).build();
    }


    @Override
    protected DatumReader<GenericRecord> buildDatumReader() {
        return reader == null ? new ThreadSpecificGenericReader() : reader ;
    }

}
