package org.sv.flexobject.hadoop.streaming.parquet.read;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import java.util.Map;

public abstract class SchemedReadSupport<T> extends ReadSupport<T> {

    MessageType schema;

    public SchemedReadSupport(MessageType schema) {
        this.schema = schema;
    }

    public abstract SchemedGroupConverter<T> newGroupConverter(MessageType schema);

    @Override
    public ReadContext init(InitContext context) {
        return new ReadContext(schema != null ? schema : context.getFileSchema());
    }

    @Override
    public RecordMaterializer<T> prepareForRead(Configuration configuration, Map<String, String> map, MessageType messageType, ReadContext readContext) {
        RecordMaterializer<T> materializer = new RecordMaterializer<T>() {
            SchemedGroupConverter<T> root = newGroupConverter(readContext.getRequestedSchema());

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
