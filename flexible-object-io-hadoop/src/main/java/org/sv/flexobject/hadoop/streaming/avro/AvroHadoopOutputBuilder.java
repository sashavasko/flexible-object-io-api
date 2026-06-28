package org.sv.flexobject.hadoop.streaming.avro;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.sv.flexobject.avro.write.OutputBuilder;

import java.io.IOException;

abstract public class AvroHadoopOutputBuilder<SELF extends AvroHadoopOutputBuilder, T> extends OutputBuilder<SELF, T> {
    protected Configuration configuration;
    protected Path filePath;

    public AvroHadoopOutputBuilder() {
    }

    public SELF withConf(Configuration conf){
        this.configuration = conf;
        return (SELF)this;
    }

    public SELF forOutput(Path filePath){
        this.filePath = filePath;
        return (SELF)this;
    }

    protected void ensureOutput() throws IOException {
        super.ensureOutput();
        if (outputStream == null) {
            outputStream = filePath.getFileSystem(configuration).create(filePath, overwrite);
        }
    }
}
