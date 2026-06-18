package org.sv.flexobject.hadoop.streaming.avro.write;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.hadoop.streaming.avro.AvroSchema;
import org.sv.flexobject.schema.SchemaException;

import java.io.IOException;
import java.io.OutputStream;

abstract public class OutputBuilder<SELF extends OutputBuilder, T> {
    protected Configuration configuration;
    protected Schema avroSchema;
    protected Class<? extends Streamable> dataClass;
    protected Path filePath;
    protected Boolean overwrite;
    protected OutputStream outputStream;

    public OutputBuilder() {
    }

    public SELF withConf(Configuration conf){
        this.configuration = conf;
        return (SELF)this;
    }

    public SELF withSchema(Class<? extends Streamable> dataClass){
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
        getAvroSchema();
        if (outputStream == null) {
            outputStream = filePath.getFileSystem(configuration).create(filePath, overwrite);
        }
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
