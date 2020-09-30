package org.sv.flexobject.hadoop.streaming.parquet.read;

import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.hadoop.streaming.parquet.ParquetSchema;

public abstract class SchemedParquetReaderBuilder<T,SELF extends ParquetReader.Builder<T>> extends ParquetReader.Builder<T> {

    MessageType schema;

    public SchemedParquetReaderBuilder(InputFile file) {
        super(file);
    }

    protected abstract SELF self();

    public SELF withSchema(MessageType schema){
        this.schema = schema;
        return self();
    }

    public SELF withSchema(Class<? extends StreamableWithSchema> dataClass){
        this.schema = ParquetSchema.forClass(dataClass);;
        return self();
    }

    public MessageType getSchema() {
        return schema;
    }
}
