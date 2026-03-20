package org.sv.flexobject.hadoop;


import org.junit.jupiter.api.Test;
import org.sv.flexobject.schema.DataTypes;
import org.sv.flexobject.schema.annotations.ScalarFieldTyped;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

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
}