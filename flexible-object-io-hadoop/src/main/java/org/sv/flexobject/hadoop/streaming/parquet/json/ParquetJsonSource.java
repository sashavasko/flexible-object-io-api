package org.sv.flexobject.hadoop.streaming.parquet.json;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopCodecs;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.hadoop.streaming.ConfiguredSource;
import org.sv.flexobject.hadoop.streaming.parquet.ParquetSchema;
import org.sv.flexobject.hadoop.streaming.parquet.ParquetSourceConf;
import org.sv.flexobject.hadoop.streaming.parquet.read.SchemedParquetReaderBuilder;
import org.sv.flexobject.hadoop.streaming.parquet.read.json.JsonParquetReaderBuilder;

import java.io.IOException;
import java.util.stream.Stream;

public class ParquetJsonSource<SELF extends ParquetJsonSource> extends ConfiguredSource<JsonNode> {

    private ParquetSourceConf conf;
    MessageType schema;

    private ParquetReader<JsonNode> parquetReader;
    private boolean isEOF = false;

    SchemedParquetReaderBuilder builder = null;

    public ParquetJsonSource() {
        conf = new ParquetSourceConf();
    }

    public ParquetJsonSource(byte[] data) {
        this();
        builder = new JsonParquetReaderBuilder(data);
    }

    public ParquetJsonSource(String namespace) {
        conf = new ParquetSourceConf(namespace);
    }

    public SELF withConf(Configuration conf){
        setConf(conf);
        return (SELF)this;
    }

    public SELF withSchema(Class<? extends StreamableWithSchema> dataClass){
        if (dataClass != null)
            schema = ParquetSchema.forClass(dataClass);
        return (SELF)this;
    }

    public SELF withSchema(MessageType schema){
        this.schema = schema;
        return (SELF) this;
    }

    public SELF forInput(String filePath){
        conf.filePath = filePath;
        return (SELF) this;
    }

    public SELF forInput(Path filePath){
        return forInput(filePath.toString());
    }

    public SELF forInput(InputFile file){
        builder = new JsonParquetReaderBuilder(file);
        return (SELF) this;
    }

    @Override
    public void setConf(Configuration configuration) {
        super.setConf(configuration);
        if (configuration != null){
            try {
                close();
            } catch (Exception e) {
                if (e instanceof RuntimeException)
                    throw (RuntimeException)e;
                else
                    throw new RuntimeException("Failed to close on configuration change", e);
            }
            isEOF = false;
            conf.from(configuration);
            withSchema(conf.getDataClass());
        }
    }

    public CompressionCodecFactory getCodecFactory() {
        return HadoopCodecs.newFactory(getConf(), 0);
    }

    ParquetReader<JsonNode> getParquetReader() throws Exception {
        if (parquetReader != null)
            return parquetReader;

        if (builder == null) {
            try {
                Path filePath = conf.getFilePath();
                if (filePath != null)
                    builder = new JsonParquetReaderBuilder(getConf(), filePath);
                else
                    throw new IllegalArgumentException("Input source must be configured with either a Configuration object or byte[]");
            } catch (IOException e) {
                throw new RuntimeException("Failed to read input file :" + conf.getFilePath(), e);
            }
        }
        parquetReader = builder
                .withSchema(schema)
                .withConf(getConf())
                .withFilter(conf.getFilter())
                .withCodecFactory(getCodecFactory()).build();
        return parquetReader;
    }

    @Override
    public JsonNode get() throws Exception {
        if (isEOF)
            return null;

        JsonNode jsonNode = getParquetReader().read();
        if (jsonNode == null)
            setEOF();
        return jsonNode;
    }

    @Override
    public void setEOF() {
        isEOF = true;
    }

    @Override
    public boolean isEOF() {
        return isEOF;
    }

    @Override
    public void close() throws Exception {
        if (parquetReader != null) {
            try {
                parquetReader.close();
                parquetReader = null;
            } catch (IOException e) {
                throw new RuntimeException("Failed to close previous reader", e);
            }
        }
    }

    @Override
    public Stream<JsonNode> stream() {
        return null;
    }

}
