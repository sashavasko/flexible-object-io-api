package org.sv.flexobject.hadoop.mapreduce.util.cacheable;


import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.hadoop.streaming.parquet.read.streamable.ParquetReaderBuilder;
import org.sv.flexobject.schema.Schema;
import org.sv.flexobject.schema.SchemaElement;

import java.io.IOException;

public abstract class HadoopCacheableParquet<T extends Streamable> extends HadoopCacheable {

    protected Class<? extends Streamable> schema;

    public HadoopCacheableParquet(Path locationOnHDFS, String counterTitle, String tag, Class<? extends Streamable> schema) {
        super(locationOnHDFS, counterTitle, tag);
        this.schema = schema;
    }

    public void loadParquet(ParquetReader<T> parquetReader) throws IOException {
        T item;

        while ((item = parquetReader.read()) != null) {
            addItem(item);
            counter.increment("items loaded");
        }
    }

    protected ParquetReader.Builder getBuilder(String filename, String path){
        try {
            return ParquetReaderBuilder
                    .forPath(getConf(), new Path(path))
                    .withSchema(schema);
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public void loadCachedFile(String filename, String path) throws IOException {
        try(ParquetReader<T> parquetReader = getBuilder(filename, path).build()) {
            loadParquet(parquetReader);
            counter.increment("files loaded");
        }
    }

    @Override
    public HadoopCacheable add(String combination) {
        String[] parts = combination.split("\\t");

        try {
            T item = (T) schema.newInstance();
            SchemaElement[] fields = Schema.getRegisteredSchema(schema).getFields();

            for (int i = 0 ; i < fields.length && i < parts.length ; ++i)
                fields[i].getDescriptor().set(item, parts[i]);
            addItem(item);

        } catch (Exception e) {
            logger.error("Failed to instantiate item for schema " + schema.getName() + ". Make sure it has public default constructor", e);
            return this;
        }

        return this;
    }

    protected abstract void addItem(T item);
}
