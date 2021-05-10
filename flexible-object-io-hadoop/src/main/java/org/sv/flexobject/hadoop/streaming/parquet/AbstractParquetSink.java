package org.sv.flexobject.hadoop.streaming.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.hadoop.streaming.parquet.write.SchemedParquetWriterBuilder;
import org.sv.flexobject.hadoop.streaming.parquet.write.streamable.ParquetWriterBuilder;
import org.sv.flexobject.stream.Sink;

import java.io.IOException;

public class AbstractParquetSink<T> implements Sink<T>, AutoCloseable {
    protected ParquetSinkConf conf;
    protected ParquetWriter<T> writer;

    private boolean hasOutput = false;

    protected AbstractParquetSink(ParquetSinkConf conf, ParquetWriter<T> writer) {
        this.conf = conf;
        this.writer = writer;
    }

    @Override
    public boolean put(T value) throws Exception {
        writer.write(value);
        hasOutput = true;
        return false;
    }

    @Override
    public boolean hasOutput() {
        return hasOutput;
    }

    @Override
    public void close() throws Exception {
        if (writer != null){
            writer.close();
            writer = null;
        }
    }

    @Override
    public void setEOF() {
        try {
            close();
        } catch (Exception e) {
            if (e instanceof RuntimeException)
                throw (RuntimeException)e;
            throw new RuntimeException(e);
        }
    }

    public abstract static class Builder<T,SINK>{
        String namespace;
        Configuration conf;
        MessageType schema;
        Class<? extends StreamableWithSchema> dataClass;
        Path filePath;
        OutputFile file;

        public Builder<T,SINK> nameSpace(String namespace){
            this.namespace = namespace;
            return this;
        }

        public Builder<T,SINK> withConf(Configuration conf){
            this.conf = conf;
            return this;
        }

        public Builder<T,SINK> withSchema(Class<? extends StreamableWithSchema> dataClass){
            this.dataClass = dataClass;
            return this;
        }

        public Builder<T,SINK> withSchema(MessageType schema){
            this.schema = schema;
            return this;
        }

        public Builder<T,SINK> forOutput(String filePath){
            return forOutput(new Path(filePath));
        }

        public Builder<T,SINK> forOutput(Path filePath){
            this.filePath = filePath;
            return this;
        }

        public Builder<T,SINK> forOutput(OutputFile file){
            this.file = file;
            return this;
        }

        protected abstract SchemedParquetWriterBuilder<T, ?> makeBuilder(Path filePath);
        protected abstract SchemedParquetWriterBuilder<T, ?> makeBuilder(OutputFile file);
        protected abstract SINK makeInstance(ParquetSinkConf conf, ParquetWriter<T> writer);

        public SINK build() throws IOException {
            ParquetSinkConf parquetConf  = null;
            ParquetWriter<T> writer = null;
            SchemedParquetWriterBuilder<T, ?> parquetBuilder = null;

            if (conf == null)
                conf = new Configuration();

            if(namespace != null)
                parquetConf = new ParquetSinkConf(namespace);
            else
                parquetConf = new ParquetSinkConf();

            parquetConf.from(conf);

            if (file != null)
                parquetBuilder = makeBuilder(file);
            else if (filePath != null)
                parquetBuilder = makeBuilder(filePath);
            else if (parquetConf.filePathIsSet())
                parquetBuilder = makeBuilder(parquetConf.getFilePath());
            else
                throw new IllegalArgumentException("Output is not set");

            if(schema == null){
                if (dataClass != null)
                    schema = ParquetSchema.forClass(dataClass);
                else if (parquetConf.hasParquetSchema())
                    schema = parquetConf.getParquetSchema();
                else
                    throw new IllegalArgumentException("Output Schema is not set");
            }

            parquetBuilder.withConf(conf);
            parquetBuilder.withSchema(schema);
            return makeInstance(parquetConf, parquetBuilder.build());
        }
    }
}