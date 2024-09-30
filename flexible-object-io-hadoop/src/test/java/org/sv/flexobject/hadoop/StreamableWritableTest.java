package org.sv.flexobject.hadoop;

import org.sv.flexobject.hadoop.streaming.avro.AvroSchema;
import org.junit.Test;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.schema.annotations.ScalarFieldTyped;

import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;

public class StreamableWritableTest {

    public static class TextByteArrayJson implements StreamableWritable {
        @ScalarFieldTyped(type = DataTypes.binary)
        public byte[] modifiedRawRecord;
    }

    @Test
    public void toFromJsonBytes() throws Exception {
        TextByteArrayJson to = new TextByteArrayJson();
        String testText = "ThisIsTheText\0This was 0";
        to.modifiedRawRecord = testText.getBytes();

        TextByteArrayJson from = new TextByteArrayJson();
        byte[] jsonBytes = to.toBytes();

        Arrays.fill(to.modifiedRawRecord, (byte)0);

        byte[] jsonBytes2 = to.toBytes();

        from.fromBytes(jsonBytes);

        System.out.println("json length:" +jsonBytes.length);
        System.out.println(from.toJson());
        assertArrayEquals(testText.getBytes(), from.modifiedRawRecord);
    }

    @Test
    public void toFromAvroBytes() throws Exception {
        System.out.println(AvroSchema.forClass(TextByteArrayAvro.class));

        TextByteArrayAvro to = new TextByteArrayAvro();
        String testText = "ThisIsTheText\0This was 0";
        to.modifiedRawRecord = testText.getBytes();

        TextByteArrayAvro from = new TextByteArrayAvro();
        byte[] avroBytes = to.toBytes();

        Arrays.fill(to.modifiedRawRecord, (byte)0);

        from.fromBytes(avroBytes);

        System.out.println(new String(avroBytes));

        System.out.println("avro length:" + avroBytes.length);
        System.out.println(from.toJson());
        assertArrayEquals(testText.getBytes(), from.modifiedRawRecord);
    }

}