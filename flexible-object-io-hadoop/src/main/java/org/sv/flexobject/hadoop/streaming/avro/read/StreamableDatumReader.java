package org.sv.flexobject.hadoop.streaming.avro.read;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.Decoder;
import org.sv.flexobject.hadoop.streaming.avro.StreamableAvroRecord;

import java.io.IOException;

public class StreamableDatumReader extends GenericDatumReader<StreamableAvroRecord> {
    public StreamableDatumReader(Schema avroSchema) {
        super(null, null, StreamableGenericData.get());
        setSchema(avroSchema);
    }

    // Just always read String - we don't need Utf8 or CharSequence
    @Override
    protected Object readString(Object old, Schema expected, Decoder in) throws IOException {
        return in.readString();
    }

    @Override
    protected Object readString(Object old, Decoder in) throws IOException {
        return in.readString();
    }

    @Override
    protected void addToMap(Object map, Object key, Object value) {
        if (value instanceof StreamableAvroRecord streamableAvroRecord) {
            value = streamableAvroRecord.getWrapped();
        }
        super.addToMap(map, key, value);
    }

    @Override
    protected void addToArray(Object array, long pos, Object e) {
        if (e instanceof StreamableAvroRecord streamableAvroRecord) {
            e = streamableAvroRecord.getWrapped();
        }
        super.addToArray(array, pos, e);
    }
}
