package org.sv.flexobject.hadoop.streaming.avro;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.schema.MessageType;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.hadoop.streaming.parquet.ParquetSchema;

public class AvroSchema {

    public static Configuration overrideBackwardsOptions(Configuration conf){
        conf.set("parquet.strict.typing", "false");
        conf.set("parquet.avro.add-list-element-records", "false");
        conf.set("parquet.avro.write-old-list-structure", "false");
        return conf;
    }

    public static Schema forClass(Class<? extends StreamableWithSchema> dataClass){
        Configuration conf = new Configuration();

        MessageType parquetSchema = ParquetSchema.forClass(dataClass);
        return ParquetSchema.toAvro(parquetSchema, overrideBackwardsOptions(conf));
    }

    public static Schema forClass(Class<? extends StreamableWithSchema> dataClass, Configuration conf){
        MessageType parquetSchema = ParquetSchema.forClass(dataClass);
        return ParquetSchema.toAvro(parquetSchema, overrideBackwardsOptions(conf));
    }

}
