package org.sv.flexobject.hadoop.streaming.parquet.write.output;

import java.io.ByteArrayOutputStream;

public class PositionByteArrayOutputStream extends ByteArrayOutputStream {

    long position = 0;

    public PositionByteArrayOutputStream(long blockSizeHint) {
        super((int) blockSizeHint);
    }

    @Override
    public synchronized void write(int b) {
        super.write(b);
        position++;
    }

    @Override
    public synchronized void write(byte[] b, int off, int len) {
        super.write(b, off, len);
        position += len;
    }

    @Override
    public synchronized void reset() {
        super.reset();
        position = 0;
    }

    public long getPos() {
        return position;
    }
}
