package org.sv.flexobject.hadoop.streaming.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.util.NonCopyingByteArrayOutputStream;
import org.sv.flexobject.Streamable;
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

        ByteBuffer output = outputStream.asByteBuffer();
//
//        TODO: Not sure how snappy compression is useful here
//
//        SnappyCodec codec = SnappyCodec.getInstance();
//        if (codec != null)
//            output = codec.compress(output);

//        DataFileWriter way
//        try (DataFileWriter<StreamableAvroRecord> fileWriter = new DataFileWriter<>(writer)) {
//            fileWriter.create(schema, outputStream);
//            fileWriter.append(wrapped);
//        }
        return output;
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

        BinaryDecoder datumIn = DecoderFactory.get().binaryDecoder(bytes, null);
        DatumReader<GenericRecord> reader = new GenericDatumReader<>(null, null, StreamableGenericData.get());
        reader.setSchema(avroSchema);
        StreamableAvroRecord wrapped = new StreamableAvroRecord(data, avroSchema);
        reader.read(wrapped, datumIn);
//        GenericRecord record = reader.read(null, datumIn);
//        AvroSchema.convertGenericRecord(record, avroSchema, data);
//
//        try(DataFileReader<GenericRecord> reader = GenericInputBuilder.forData(bytes, dataClass)) {
//            GenericRecord record = reader.next();
//            AvroSchema.convertGenericRecord(record, record.getSchema(), data);
//        }
        return data;
    }
}
