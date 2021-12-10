package org.sv.flexobject.hadoop.streaming.avro.write;

import org.sv.flexobject.Streamable;
import org.sv.flexobject.hadoop.streaming.avro.StreamableAvroRecord;

public class AvroSink<T extends Streamable> extends AvroGenericSink<StreamableAvroRecord, T> {

    @Override
    protected StreamableAvroRecord wrap(T originalValue) {
        return new StreamableAvroRecord(originalValue, builder().getAvroSchema());
    }
}
