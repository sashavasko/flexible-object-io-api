package org.sv.flexobject.hadoop.streaming.parquet.read.input;

import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class SeekableByteArray extends ByteArrayInputStream implements Seekable, PositionedReadable {

    public SeekableByteArray(byte[] buf)
    {
        super(buf);
    }

    @Override
    public long getPos() throws IOException {
        return pos;
    }

    @Override
    public void seek(long pos) throws IOException {
        if (mark != 0)
            throw new IllegalStateException();

        reset();
        long skipped = skip(pos);

        if (skipped != pos)
            throw new IOException();
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        return false;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) throws IOException {

        if (position >= buf.length)
            throw new IllegalArgumentException();
        if (position + length > buf.length)
            throw new IllegalArgumentException();
        if (length > buffer.length)
            throw new IllegalArgumentException();

        System.arraycopy(buf, (int) position, buffer, offset, length);
        return length;
    }

    public void readFully(byte[] buffer) throws IOException {
        read(0, buffer, 0, buffer.length);
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
        read(position, buffer, 0, buffer.length);
    }

    public void readFully(byte[] buffer, int offset, int length) throws IOException {
        read(0, buffer, offset, length);
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
        read(position, buffer, offset, length);
    }

    public long getLength() {
        return count;
    }
}

