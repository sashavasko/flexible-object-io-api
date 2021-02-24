package org.sv.flexobject.hadoop.streaming.avro.write;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.hadoop.streaming.avro.AvroSchema;

import java.io.IOException;
import java.io.OutputStream;

abstract public class OutputBuilder<SELF extends OutputBuilder, T> {
    protected Configuration configuration;
    protected Schema avroSchema;
    protected Class<? extends StreamableWithSchema> dataClass;
    protected Path filePath;
    protected Boolean overwrite;
    protected OutputStream outputStream;

    public OutputBuilder() {
    }

    public SELF withConf(Configuration conf){
        this.configuration = conf;
        return (SELF)this;
    }

    public SELF withSchema(Class<? extends StreamableWithSchema> dataClass){
        this.dataClass = dataClass;
        return (SELF)this;
    }

    public SELF withSchema(Schema avroSchema){
        this.avroSchema = avroSchema;
        return (SELF)this;
    }

    public SELF forOutput(Path filePath){
        this.filePath = filePath;
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
        if (avroSchema == null) {
            avroSchema = AvroSchema.forClass(dataClass);
        }
        if (outputStream == null) {
            outputStream = filePath.getFileSystem(configuration).create(filePath, overwrite);
        }
    }

    public Schema getAvroSchema() {
        return avroSchema;
    }
}
