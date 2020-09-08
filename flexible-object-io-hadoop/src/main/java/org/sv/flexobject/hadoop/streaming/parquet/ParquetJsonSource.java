package org.sv.flexobject.hadoop.streaming.parquet;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopCodecs;
import org.sv.flexobject.hadoop.streaming.ConfiguredSource;
import org.sv.flexobject.hadoop.streaming.parquet.read.json.JsonParquetReaderBuilder;

import java.io.IOException;
import java.util.stream.Stream;

public class ParquetJsonSource extends ConfiguredSource<JsonNode> {

    private ParquetSourceConf conf;

    private ParquetReader<JsonNode> parquetReader;
    private boolean isEOF = false;

    ParquetReader.Builder builder = null;

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

    @Override
    public void setConf(Configuration conf) {
        super.setConf(conf);
        if (conf != null){
            try {
                close();
            } catch (Exception e) {
                if (e instanceof RuntimeException)
                    throw (RuntimeException)e;
                else
                    throw new RuntimeException("Failed to close on configuration change", e);
            }
            isEOF = false;
            this.conf.from(conf);
            try {
                builder = new JsonParquetReaderBuilder(conf, this.conf.getFilePath())
                        .withFilter(this.conf.getFilter())
                        .withCodecFactory(getCodecFactory());
            } catch (IOException e) {
                throw new RuntimeException("Failed to read input file :" + this.conf.getFilePath(), e);
            }
        }
    }

    public CompressionCodecFactory getCodecFactory() {
        return HadoopCodecs.newFactory(getConf(), 0);
    }

    ParquetReader<JsonNode> getParquetReader() throws Exception {
        if (parquetReader != null)
            return parquetReader;

        if (builder == null)
            throw new IllegalArgumentException("Input source must be configured with either a Configuration object or byte[]");

        parquetReader = builder.build();
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
