package org.sv.flexobject.hadoop.streaming.parquet.write;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;
import org.sv.flexobject.hadoop.streaming.parquet.ParquetSchema;

public abstract class SchemedParquetWriterBuilder<T,SELF extends ParquetWriter.Builder<T, SELF>> extends ParquetWriter.Builder<T, SELF> {

    MessageType schema;

    public SchemedParquetWriterBuilder(Path path) {
        super(path);
    }

    public SchemedParquetWriterBuilder(OutputFile file) {
        super(file);
    }

    public SELF withSchema(MessageType schema){
        this.schema = schema;
        return self();
    }

    public SELF withSchema(Class<?> dataClass){
        this.schema = ParquetSchema.forClass(dataClass);
        return self();
    }

    public MessageType getSchema() {
        return schema;
    }
}
