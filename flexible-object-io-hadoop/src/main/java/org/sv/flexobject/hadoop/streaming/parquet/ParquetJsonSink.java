package org.sv.flexobject.hadoop.streaming.parquet;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.hadoop.streaming.parquet.write.json.JsonParquetWriterBuilder;
import org.sv.flexobject.stream.Sink;

import java.io.IOException;

public class ParquetJsonSink extends Configured implements Sink<JsonNode>, AutoCloseable {
    ParquetSinkConf conf;
    MessageType schema;

    JsonParquetWriterBuilder builder;

    ParquetWriter<JsonNode> writer;
    boolean hasOutput = false;

    public ParquetJsonSink() {
        conf = new ParquetSinkConf();
    }

    public ParquetJsonSink(String namespace) {
        conf = new ParquetSinkConf(namespace);
    }

    public ParquetJsonSink withConf(Configuration conf){
        setConf(conf);
        return this;
    }

    public ParquetJsonSink withSchema(Class<? extends StreamableWithSchema> dataClass){
        if (dataClass != null)
            schema = ParquetSchema.forClass(dataClass);
        return this;
    }

    public ParquetJsonSink withSchema(MessageType schema){
        this.schema = schema;
        return this;
    }

    public ParquetJsonSink forOutput(String filePath){
        return forOutput(new Path(filePath));
    }

    public ParquetJsonSink forOutput(Path filePath){
        builder = new JsonParquetWriterBuilder(filePath);
        return this;
    }

    public ParquetJsonSink forOutput(OutputFile file){
        builder = new JsonParquetWriterBuilder(file);
        return this;
    }

    @Override
    public void setConf(Configuration configuration) {
        super.setConf(configuration);
        if (conf != null){
            try {
                close();
            } catch (Exception e) {
                if (e instanceof RuntimeException)
                    throw (RuntimeException)e;
                else
                    throw new RuntimeException("Failed to close on configuration change", e);
            }
            conf.from(configuration);
            withSchema(conf.getDataClass());
        }
    }

    @Override
    public boolean put(JsonNode value) throws Exception {
        getWriter().write(value);
        hasOutput = true;
        return false;
    }

    public ParquetWriter<JsonNode> getWriter() {
        if (writer != null)
            return writer;

        if (builder == null) {
            if (conf.getFilePath() != null) {
                builder = new JsonParquetWriterBuilder(conf.getFilePath());
            }else
                throw new RuntimeException("Parquet output must be configured with either OutputFile or path");
        }

        try {
            writer = builder
                    .withSchema(schema)
                    .withConf(getConf()).build();
        } catch (IOException e) {
            throw new RuntimeException("Failed to start Parquet output: ", e);
        }

        return writer;
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
}
