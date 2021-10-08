package org.sv.flexobject.hadoop.streaming.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.hadoop.streaming.parquet.read.SchemedParquetReaderBuilder;
import org.sv.flexobject.stream.Source;
import org.sv.flexobject.util.InstanceFactory;

import java.io.IOException;
import java.util.stream.Stream;

public class AbstractParquetSource<T> implements Source<T>, AutoCloseable {
    protected ParquetSourceConf conf;
    protected ParquetReader<T> reader;

    protected boolean isEOF = false;

    protected AbstractParquetSource(){};

    @Override
    public T get() throws Exception {
        T record = reader.read();
        if (record == null)
            isEOF = true;
        return record;
    }

    @Override
    public void close() throws Exception {
        if (reader != null){
            reader.close();
            reader = null;
        }
    }

    @Override
    public Stream<T> stream() {
        return null;
    }

    @Override
    public boolean isEOF() {
        return isEOF;
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

    public abstract static class Builder<T, SOURCE extends AbstractParquetSource>{
        String namespace;
        Configuration conf;
        MessageType schema;
        Class<? extends StreamableWithSchema> dataClass;
        Path filePath;
        InputFile file;
        Class<? extends AbstractParquetSource> sourceClass;

        public Builder(Class<? extends AbstractParquetSource> sourceClass) {
            this.sourceClass = sourceClass;
        }

        public Builder<T, SOURCE> nameSpace(String namespace){
            this.namespace = namespace;
            return this;
        }

        public Builder<T, SOURCE> withConf(Configuration conf){
            this.conf = conf;
            return this;
        }

        public Builder<T, SOURCE> withSchema(Class<? extends StreamableWithSchema> dataClass){
            this.dataClass = dataClass;
            return this;
        }

        public Builder<T, SOURCE> withSchema(MessageType schema){
            this.schema = schema;
            return this;
        }

        public Builder<T, SOURCE> forInput(String filePath){
            return forInput(new Path(filePath));
        }

        public Builder<T, SOURCE> forInput(Path filePath){
            this.filePath = filePath;
            return this;
        }

        public Builder<T, SOURCE> forInput(InputFile file){
            this.file = file;
            return this;
        }

        protected abstract SchemedParquetReaderBuilder<T, ?> makeBuilder(Configuration conf, Path filePath) throws IOException;
        protected abstract SchemedParquetReaderBuilder<T, ?> makeBuilder(InputFile file);

        public SOURCE build() throws IOException {
            ParquetSourceConf parquetConf  = null;
            ParquetReader<T> reader = null;
            SchemedParquetReaderBuilder<T, ?> parquetBuilder = null;

            if (conf == null)
                conf = new Configuration();

            if(namespace != null)
                parquetConf = new ParquetSourceConf(namespace);
            else
                parquetConf = new ParquetSourceConf();

            parquetConf.from(conf);

            if (file != null)
                parquetBuilder = makeBuilder(file);
            else {
                if (filePath == null && parquetConf.filePathIsSet())
                    parquetBuilder = makeBuilder(conf, parquetConf.getFilePath());
                else {
                    if (filePath == null)
                        throw new IllegalArgumentException("Input is not set");
                    parquetBuilder = makeBuilder(conf, filePath);
                }
            }

            if(schema == null){
                if (dataClass != null)
                    schema = ParquetSchema.forClass(dataClass);
                else if (parquetConf.hasParquetSchema())
                    schema = parquetConf.getParquetSchema();
//                else
//                    throw new IllegalArgumentException("Input Schema is not set");
            }

            parquetBuilder.withConf(conf);
            if (schema != null)
                parquetBuilder.withSchema(schema);

            SOURCE source = (SOURCE) sourceClass.cast(InstanceFactory.get(sourceClass));
            source.conf = parquetConf;
            source.reader = parquetBuilder.build();
            return source;
        }
    }
}
