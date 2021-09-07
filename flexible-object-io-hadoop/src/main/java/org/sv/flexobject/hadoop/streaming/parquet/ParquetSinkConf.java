package org.sv.flexobject.hadoop.streaming.parquet;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.schema.MessageType;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.hadoop.properties.HadoopPropertiesWrapper;

public final class ParquetSinkConf extends HadoopPropertiesWrapper<ParquetSinkConf> {

    public static final String SUBNAMESPACE = "output.streaming.parquet";

    public String filePath;
    public JsonNode schemaJson;
    public Class<? extends StreamableWithSchema> dataClass;

    public ParquetSinkConf() {
        super(SUBNAMESPACE);
    }

    @Override
    public ParquetSinkConf setDefaults() {
        return this;
    }

    public ParquetSinkConf(String namespace) {
        super(namespace);
    }

    public boolean filePathIsSet(){
        return filePath != null;
    }

    public Path getFilePath(){
        return filePath == null ? null : new Path(filePath);
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
