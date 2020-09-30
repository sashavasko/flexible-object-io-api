package org.sv.flexobject.hadoop.streaming.parquet.read;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Type;

public class SchemedPrimitiveConverter<T> extends PrimitiveConverter {

    T current;
    Type type;

    public SchemedPrimitiveConverter(Type type) {
        this.type = type;
    }

    @Override
    public boolean isPrimitive() {
        return true;
    }

    public T getCurrent() {
        return current;
    }

    public void setCurrent(T current) {
        this.current = current;
    }

    public Type getType() {
        return type;
    }

    public OriginalType getOriginalType() {
        return type.getOriginalType();
    }
}
