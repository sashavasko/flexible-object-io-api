package org.sv.flexobject.hadoop.streaming.parquet.read.input;

import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.IOException;

public class ByteArrayInputFile implements InputFile {

    SeekableByteArray byteArray;

    public ByteArrayInputFile(byte[] data) {
        byteArray = new SeekableByteArray(data);
    }

    @Override
    public long getLength() throws IOException {
        return byteArray.getLength();
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
        return new DelegatingSeekableInputStream(byteArray) {
            @Override
            public long getPos() throws IOException {
                return byteArray.getPos();
            }

            @Override
            public void seek(long newPos) throws IOException {
                byteArray.seek(newPos);
            }
        };
    }
}
