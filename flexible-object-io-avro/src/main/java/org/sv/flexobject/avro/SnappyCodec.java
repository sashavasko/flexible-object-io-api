package org.sv.flexobject.avro;

import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

public class SnappyCodec {

    /** Partly copied from Avro library as it is package-private there */

    private final CRC32 crc32 = new CRC32();
    private SnappyCodec() {
    }

    public static SnappyCodec getInstance(){
        try{
            Snappy.getNativeLibraryVersion();
            return new SnappyCodec();
        } catch(Exception ignored) {
            return null;
        }
    }

    static int computeOffset(ByteBuffer data) {
        return data.arrayOffset() + data.position();
    }

    public ByteBuffer compress(ByteBuffer in) throws IOException {
        int offset = computeOffset(in);
        ByteBuffer out = ByteBuffer.allocate(Snappy.maxCompressedLength(in.remaining()) + 4);
        int size = Snappy.compress(in.array(), offset, in.remaining(), out.array(), 0);
        crc32.reset();
        crc32.update(in.array(), offset, in.remaining());
        out.putInt(size, (int) crc32.getValue());

        ((Buffer) out).limit(size + 4);

        return out;
    }

    public ByteBuffer decompress(ByteBuffer in) throws IOException {
        int offset = computeOffset(in);
        ByteBuffer out = ByteBuffer.allocate(Snappy.uncompressedLength(in.array(), offset, in.remaining() - 4));
        int size = Snappy.uncompress(in.array(), offset, in.remaining() - 4, out.array(), 0);
        ((Buffer) out).limit(size);

        crc32.reset();
        crc32.update(out.array(), 0, size);
        if (in.getInt(((Buffer) in).limit() - 4) != (int) crc32.getValue())
            throw new IOException("Checksum failure");

        return out;
    }

}
