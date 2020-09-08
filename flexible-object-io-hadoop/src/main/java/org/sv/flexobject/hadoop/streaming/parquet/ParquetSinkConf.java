package org.sv.flexobject.hadoop.streaming.parquet;

import org.apache.hadoop.fs.Path;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.hadoop.properties.HadoopPropertiesWrapper;

public class ParquetSinkConf extends HadoopPropertiesWrapper<ParquetSinkConf> {

    public static final String SUBNAMESPACE = "output.streaming.parquet";

    String filePath;
    Class<? extends StreamableWithSchema> dataClass;

    public ParquetSinkConf() {
    }

    public ParquetSinkConf(String namespace) {
        super(namespace);
    }

    @Override
    public String getSubNamespace() {
        return SUBNAMESPACE;
    }

    public Path getFilePath(){
        return filePath == null ? null : new Path(filePath);
    }

    public Class<? extends StreamableWithSchema> getDataClass() {
        return dataClass;
    }
}
