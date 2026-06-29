package org.sv.flexobject.hadoop;

import org.junit.Test;
import org.sv.flexobject.avro.AvroSchema;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.schema.annotations.ScalarFieldTyped;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;

public class StreamableWritableTest {

    public static class TextByteArrayJson implements StreamableWritable {
        @ScalarFieldTyped(type = DataTypes.binary)
        public byte[] modifiedRawRecord;

        @Override
        public Strategy getStrategy() throws IOException {
            return Strategy.json;
        }
    }

    @Test
    public void toFromJsonBytes() throws Exception {
        TextByteArrayJson to = new TextByteArrayJson();
        String testText = "ThisIsTheText\0This was 0";
        to.modifiedRawRecord = testText.getBytes();

        TextByteArrayJson from = new TextByteArrayJson();
        byte[] jsonBytes = to.toBytes();

//        Arrays.fill(to.modifiedRawRecord, (byte)0);
//
//        byte[] jsonBytes2 = to.toBytes();

        from.fromBytes(jsonBytes);

        System.out.println("json length:" +jsonBytes.length);
        System.out.println("to:   " + to.toJson());
        System.out.println("from: " + from.toJson());
        assertArrayEquals(testText.getBytes(), from.modifiedRawRecord);
    }

    @Test
    public void toFromAvroBytes() throws Exception {
        System.out.println(AvroSchema.forClass(TextByteArrayAvro.class));

        TextByteArrayAvro to = new TextByteArrayAvro();
        String testText = "ThisIsTheText This was 0";
        to.modifiedRawRecord = testText.getBytes();
        System.out.println("to:   " + to.toJson());

        byte[] avroBytes = to.toBytes();
        Arrays.fill(to.modifiedRawRecord, (byte)0);

        TextByteArrayAvro from = new TextByteArrayAvro();
        from.fromBytes(avroBytes);

        System.out.println("from: " + from.toJson());
        System.out.println(new String(avroBytes));
        System.out.println("avro length:" + avroBytes.length);
        System.out.println(from.toJson());
        assertArrayEquals(testText.getBytes(), from.modifiedRawRecord);
    }

}