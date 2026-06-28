package org.sv.flexobject.avro.read;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.file.SeekableFileInput;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.avro.AvroSchema;

import java.io.File;
import java.io.IOException;

public class InputBuilder<SELF extends InputBuilder, T> {

    protected Schema avroSchema;
    protected Class<? extends Streamable> dataClass;
    protected File file;
    protected byte[] bytes;
    protected SeekableInput input;

    public InputBuilder() {
    }

    public SELF withSchema(Class<? extends Streamable> dataClass) {
        this.dataClass = dataClass;
        return (SELF) this;
    }

    public SELF withSchema(Schema avroSchema) {
        this.avroSchema = avroSchema;
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
        }
    }

    protected void ensureSchema() {
        if (avroSchema == null) {
            avroSchema = AvroSchema.forClass(dataClass);
        }
    }
}
