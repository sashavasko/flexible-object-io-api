package org.sv.flexobject.hadoop.streaming.parquet;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.hadoop.properties.HadoopPropertiesWrapper;

public class ParquetSourceConf extends HadoopPropertiesWrapper<ParquetSourceConf> {

    public static final String SUBNAMESPACE = "input.streaming.parquet";

    public String filePath;
    public Class<? extends StreamableWithSchema> dataClass;

    public ParquetSourceConf() {
        super();
    }

    @Override
    public ParquetSourceConf setDefaults() {
        return this;
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
