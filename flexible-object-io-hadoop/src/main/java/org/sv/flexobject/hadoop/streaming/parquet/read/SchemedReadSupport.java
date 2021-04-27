package org.sv.flexobject.hadoop.streaming.parquet.read;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.sv.flexobject.hadoop.streaming.parquet.ParquetSchema;
import org.sv.flexobject.hadoop.streaming.parquet.ParquetSchemaConf;

import java.util.Map;

public abstract class SchemedReadSupport<T> extends ReadSupport<T> {
    MessageType schema = null;

    public SchemedReadSupport() {
    }

    public SchemedReadSupport(MessageType schema) {
        this.schema = schema;
    }

    public abstract SchemedGroupConverter<T> newGroupConverter(MessageType schema, MessageType fileSchema);

    @Override
    public ReadContext init(InitContext context) {
        if (schema == null) {
            ParquetSchemaConf conf = new ParquetSchemaConf().from(context.getConfiguration());
            schema = conf.getInputSchema();
        }

        if (schema == null)
            return new ReadContext(context.getFileSchema());

        schema = ParquetSchema.correctSchemaForSimpleLists(schema, context.getFileSchema());

        return new ReadContext(schema);
    }

    @Override
    public RecordMaterializer<T> prepareForRead(Configuration configuration, Map<String, String> map, MessageType fileSchema, ReadContext readContext) {
        RecordMaterializer<T> materializer = new RecordMaterializer<T>() {
            SchemedGroupConverter<T> root = newGroupConverter(readContext.getRequestedSchema(), fileSchema);

            @Override
            public T getCurrentRecord() {
                return root.getCurrentRecord();
            }

            @Override
            public GroupConverter getRootConverter() {
                return root;
            }
        };
        return materializer;
    }
}
