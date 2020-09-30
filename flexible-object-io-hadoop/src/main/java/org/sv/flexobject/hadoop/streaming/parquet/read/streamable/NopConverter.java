package org.sv.flexobject.hadoop.streaming.parquet.read.streamable;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;

public class NopConverter extends StreamableConverter {

    static NopConverter instance = null;

    private NopConverter() {
        super(null);
    }

    public static NopConverter getInstance() {
        if (instance == null)
            instance = new NopConverter();
        return instance;
    }

    @Override
    public boolean isPrimitive() {
        return false;
    }

    @Override
    public GroupConverter asGroupConverter() {
        return new GroupConverter() {
            @Override
            public Converter getConverter(int i) {
                return getInstance();
            }

            @Override
            public void start() {

            }

            @Override
            public void end() {

            }
        };
    }
}
