package org.sv.flexobject.hadoop.streaming.avro;

import org.apache.avro.file.SeekableFileInput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.sv.flexobject.avro.read.GenericInputBuilder;

import java.io.IOException;

public class AvroHadoopInputBuilder<SELF extends AvroHadoopInputBuilder> extends GenericInputBuilder<SELF> {

    protected Configuration configuration;
    protected Path filePath;

    public AvroHadoopInputBuilder() {
    }

    public SELF withConf(Configuration conf) {
        this.configuration = conf;
        return (SELF) this;
    }

    public SELF forInput(Path filePath) {
        this.filePath = filePath;
        return (SELF) this;
    }

    protected void ensureInput() throws IOException {
        super.ensureInput();
        if (input == null && filePath != null) {
            FSDataInputStream is = filePath.getFileSystem(configuration).open(filePath);
            input = new SeekableFileInput(is.getFileDescriptor());
        }
    }
}
