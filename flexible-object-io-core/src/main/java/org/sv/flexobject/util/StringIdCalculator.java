package org.sv.flexobject.util;

import org.apache.commons.codec.digest.PureJavaCrc32C;

import java.util.Base64;

public class StringIdCalculator {
    public static final long MIN_ID = 0L;
    public static final long MAX_ID = 0x7fffffffffffffffL;
    PureJavaCrc32C crc32Codec = new PureJavaCrc32C();

    Long crc32c;
    Long base64crc32c;
    byte[] base64;

    public long calculate(String s){
        crc32Codec.reset();
        byte[] bytes = s.getBytes();
        crc32Codec.update(bytes, 0, bytes.length);
        crc32c = crc32Codec.getValue();
        crc32Codec.reset();
        base64 = Base64.getEncoder().encode(bytes);
        crc32Codec.update(base64, 0, base64.length);
        base64crc32c = crc32Codec.getValue();
        return (crc32c << 31) | base64crc32c;
    }

    public Long getCrc32c() {
        return crc32c;
    }

    public Long getBase64crc32c() {
        return base64crc32c;
    }

    public byte[] getBase64() {
        return base64;
    }
}
