package org.sv.flexobject.avro.write;

import org.sv.flexobject.Streamable;
import org.sv.flexobject.avro.StreamableAvroRecord;

public class AvroSink<T extends Streamable> extends AvroGenericSink<StreamableAvroRecord, T> {

    @Override
    protected StreamableAvroRecord wrap(T originalValue) {
        return new StreamableAvroRecord(originalValue, builder().getAvroSchema());
    }
}
