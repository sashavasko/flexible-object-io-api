package org.sv.flexobject.avro.write;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.avro.AvroSchema;

import java.io.IOException;
import java.io.OutputStream;

abstract public class OutputBuilder<SELF extends OutputBuilder, T> {
    protected Schema avroSchema;
    protected Class<? extends Streamable> dataClass;
    protected Boolean overwrite;
    protected OutputStream outputStream;

    public OutputBuilder() {
    }

    public SELF withSchema(Class<? extends Streamable> dataClass){
        this.dataClass = dataClass;
        return (SELF)this;
    }

    public SELF withSchema(Schema avroSchema){
        this.avroSchema = avroSchema;
        return (SELF)this;
    }

    public SELF forOutput(OutputStream outputStream){
        this.outputStream = outputStream;
        return (SELF)this;
    }

    public SELF overwrite(){
        this.overwrite = true;
        return (SELF)this;
    }

    public DataFileWriter<T> build() throws IOException {
        ensureOutput();

        final DatumWriter<T> datumWriter = buildDatumWriter();
        DataFileWriter<T> fileWriter = buildFileWriter(datumWriter);
        fileWriter.create(avroSchema, outputStream);
        return fileWriter;
    }

    abstract protected DatumWriter<T> buildDatumWriter();

    protected DataFileWriter<T> buildFileWriter(DatumWriter datumWriter) {
        return new DataFileWriter<T>(datumWriter);
    }

    protected void ensureOutput() throws IOException {
        getAvroSchema();
    }

    public Schema getAvroSchema() {
        if (avroSchema == null) {
            if (dataClass == null)
                throw new IllegalArgumentException("Either Avro schema or a class implementing Streamable must be specified");
            avroSchema = AvroSchema.forClass(dataClass);
        }
        return avroSchema;
    }
}
