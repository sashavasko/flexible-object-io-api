package org.sv.flexobject.rabbit.streaming;

import java.io.*;
import java.util.Arrays;
import java.util.zip.Deflater;

public class RabbitSerializableSink extends RabbitGenericSink<Serializable> {

    public static byte[] deflate(byte[] input, int inputSize){
        Deflater deflater = new Deflater();
        deflater.setInput(input);
        deflater.finish();
        byte[] output = new byte[inputSize*4];
        int outputSize = deflater.deflate(output);
        deflater.end();
        return Arrays.copyOf(output, outputSize);
    }

    public static byte[] encodeMessage(Object message) throws IOException {
        try(ByteArrayOutputStream os = new ByteArrayOutputStream();
            ObjectOutput objOut = new ObjectOutputStream(os)) {
            objOut.writeObject(message);
            return deflate(os.toByteArray(), os.size());
        }
    }

    public static Builder builder(){
        return RabbitGenericSink.builder()
                .sinkType(RabbitSerializableSink.class)
                .converter(RabbitSerializableSink::encodeMessage);
    }
}
