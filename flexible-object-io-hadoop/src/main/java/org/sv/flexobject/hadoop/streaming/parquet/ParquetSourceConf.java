package org.sv.flexobject.hadoop.streaming.parquet;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.schema.MessageType;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.hadoop.properties.HadoopPropertiesWrapper;

public final class ParquetSourceConf extends HadoopPropertiesWrapper<ParquetSourceConf> {
    public static final String SUBNAMESPACE = "input.streaming.parquet";

    public String filePath;
    public JsonNode schemaJson;
    public Class<? extends StreamableWithSchema> dataClass;

    @Override
    protected String getSubNamespace() {
        return SUBNAMESPACE;
    }

    public ParquetSourceConf() {
        super(SUBNAMESPACE);
    }

    @Override
    public ParquetSourceConf setDefaults() {
        return this;
    }

    public ParquetSourceConf(String namespace) {
        super(namespace);
    }

    public boolean filePathIsSet(){
        return filePath != null;
    }

    public Path getFilePath(){
        return filePath == null ? null : new Path(filePath);
    }

    public Class<? extends StreamableWithSchema> getDataClass() {
        return dataClass;
    }

    public FilterCompat.Filter getFilter() {
        return null;
    }

    public MessageType getParquetSchema() {
        if (schemaJson != null)
            return ParquetSchema.fromJson((ObjectNode) schemaJson);
        else if(dataClass != null)
            return ParquetSchema.forClass(dataClass);
        return null;
    }

    public boolean hasParquetSchema(){
        return schemaJson != null || dataClass != null;
    }
}
