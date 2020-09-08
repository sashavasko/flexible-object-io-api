package org.sv.flexobject.hadoop.streaming.parquet;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.hadoop.properties.HadoopPropertiesWrapper;

public class ParquetSourceConf extends HadoopPropertiesWrapper<ParquetSourceConf> {

    public static final String SUBNAMESPACE = "input.streaming.parquet";

    String filePath;
    Class<? extends StreamableWithSchema> dataClass;

    public ParquetSourceConf() {
    }

    public ParquetSourceConf(String namespace) {
        super(namespace);
    }

    @Override
    public String getSubNamespace() {
        return SUBNAMESPACE;
    }

    public Path getFilePath(){
        return filePath != null ?new Path(filePath) : null;
    }

    public Class<? extends StreamableWithSchema> getDataClass() {
        return dataClass;
    }

    public FilterCompat.Filter getFilter() {
        return null;
    }
}
