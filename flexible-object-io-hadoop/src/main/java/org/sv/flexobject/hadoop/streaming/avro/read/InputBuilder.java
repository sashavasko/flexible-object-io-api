package org.sv.flexobject.hadoop.streaming.avro.read;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.file.SeekableFileInput;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.hadoop.streaming.avro.AvroSchema;

import java.io.File;
import java.io.IOException;

public class InputBuilder<SELF extends InputBuilder, T> {

    protected Configuration configuration;
    protected Schema avroSchema;
    protected Class<? extends StreamableWithSchema> dataClass;
    protected File file;
    protected Path filePath;
    protected byte[] bytes;
    protected SeekableInput input;

    public InputBuilder() {
    }

    public SELF withConf(Configuration conf) {
        this.configuration = conf;
        return (SELF) this;
    }

    public SELF withSchema(Class<? extends StreamableWithSchema> dataClass) {
        this.dataClass = dataClass;
        return (SELF) this;
    }

    public SELF withSchema(Schema avroSchema) {
        this.avroSchema = avroSchema;
        return (SELF) this;
    }

    public SELF forInput(Path filePath) {
        this.filePath = filePath;
        return (SELF) this;
    }

    public SELF forInput(byte[] bytes) {
        this.bytes = bytes;
        return (SELF) this;
    }

    public SELF forInput(SeekableInput input) {
        this.input = input;
        return (SELF) this;
    }

    public SELF forInput(File input) {
        this.file = file;
        return (SELF) this;
    }

    final DataFileReader<T> build() throws IOException {
        ensureSchema();

        ensureInput();

        final DatumReader<T> reader = buildDatumReader();
        final DataFileReader<T> dataFileReader = new DataFileReader<T>(input, reader);

        return dataFileReader;
    }

    protected DatumReader<T> buildDatumReader() {
        return new GenericDatumReader<>();
    }

    protected void ensureInput() throws IOException {
        if (input == null) {
            if (bytes != null)
                input = new SeekableByteArrayInput(bytes);
            else if(file != null)
                input = new SeekableFileInput(file);
            else if (filePath != null) {
                FSDataInputStream is = filePath.getFileSystem(configuration).open(filePath);
                input = new SeekableFileInput(is.getFileDescriptor());
            }
        }
    }

    protected void ensureSchema() {
        if (avroSchema == null) {
            avroSchema = AvroSchema.forClass(dataClass);
        }
    }
}
