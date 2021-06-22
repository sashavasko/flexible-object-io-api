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
import org.sv.flexobject.util.InstanceFactory;

import java.io.IOException;

public class AbstractParquetSink<T> implements Sink<T>, AutoCloseable {
    protected ParquetSinkConf conf;
    protected ParquetWriter<T> writer;

    private boolean hasOutput = false;

    protected AbstractParquetSink() {
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

    public abstract static class Builder<T,SINK extends AbstractParquetSink>{
        String namespace;
        Configuration conf;
        MessageType schema;
        Class<? extends StreamableWithSchema> dataClass;
        Path filePath;
        boolean overwrite = false;
        OutputFile file;
        Class<? extends AbstractParquetSink> sinkClass;

        public Builder(Class<? extends AbstractParquetSink> sinkClass) {
            this.sinkClass = sinkClass;
        }

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

        public Builder<T,SINK> overwrite(){
            this.overwrite = true;
            return this;
        }

        public Builder<T,SINK> forOutput(OutputFile file){
            this.file = file;
            return this;
        }

        protected abstract SchemedParquetWriterBuilder<T, ?> makeBuilder(Path filePath);
        protected abstract SchemedParquetWriterBuilder<T, ?> makeBuilder(OutputFile file);

        protected void doOverwrite(Path filePath){
            if (overwrite) {
                try {
                    filePath.getFileSystem(conf).delete(filePath, true);
                } catch (IOException e) {
                }
            }
        }

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
            else {
                if (filePath == null && parquetConf.filePathIsSet()){
                    doOverwrite(parquetConf.getFilePath());
                    parquetBuilder = makeBuilder(parquetConf.getFilePath());
                }else {
                    if (filePath == null)
                        throw new IllegalArgumentException("Output is not set");
                    doOverwrite(filePath);
                    parquetBuilder = makeBuilder(filePath);
                }
            }

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
            SINK sink = (SINK) sinkClass.cast(InstanceFactory.get(sinkClass));
            sink.conf = parquetConf;
            sink.writer = parquetBuilder.build();
            return sink;
        }
    }
}
