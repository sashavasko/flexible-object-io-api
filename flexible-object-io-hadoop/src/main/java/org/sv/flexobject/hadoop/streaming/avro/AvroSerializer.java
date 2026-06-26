package org.sv.flexobject.hadoop.streaming.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.util.NonCopyingByteArrayOutputStream;
import org.sv.flexobject.Streamable;
import org.sv.flexobject.hadoop.streaming.avro.read.StreamableDatumReader;
import org.sv.flexobject.hadoop.streaming.avro.read.StreamableGenericData;
import org.sv.flexobject.util.InstanceFactory;

import java.io.IOException;
import java.nio.ByteBuffer;

public class AvroSerializer {

    public static ByteBuffer toBytes(Streamable data) throws IOException {
        return toBytes(data, AvroSchema.forClass(data.getClass()));
    }

    public static ByteBuffer toBytes(Streamable data, Schema schema) throws IOException {
        StreamableAvroRecord wrapped = new StreamableAvroRecord(data, schema);

        NonCopyingByteArrayOutputStream outputStream = new NonCopyingByteArrayOutputStream(1024);

        GenericDatumWriter<StreamableAvroRecord> writer = new GenericDatumWriter<>();
        writer.setSchema(schema);
        BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(outputStream,null);
        writer.write(wrapped, encoder);
        encoder.flush();

        return outputStream.asByteBuffer();
    }

    public static byte[] toBytes(ByteBuffer byteBuffer){
        byte[] bytes = new byte[byteBuffer.remaining()];
        byteBuffer.get(bytes);
        return bytes;
    }

    public static <T extends Streamable> T fromBytes(byte[] bytes, Class<? extends Streamable> dataClass) throws Exception {
        return fromBytes(bytes, dataClass, AvroSchema.forClass(dataClass));
    }

    public static <T extends Streamable> T fromBytes(byte[] bytes, Class<? extends Streamable> dataClass, Schema avroSchema) throws Exception {
        if (bytes == null)
            return null;

        @SuppressWarnings("unchecked")
        T data = (T) InstanceFactory.get(dataClass);
        StreamableAvroRecord wrapped = new StreamableAvroRecord(data, avroSchema);

        StreamableDatumReader reader = new StreamableDatumReader(avroSchema);
        BinaryDecoder datumIn = DecoderFactory.get().binaryDecoder(bytes, null);
        reader.read(wrapped, datumIn);

        return data;
    }
}
