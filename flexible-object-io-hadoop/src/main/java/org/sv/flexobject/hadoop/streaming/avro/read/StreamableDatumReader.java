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
}
