package org.sv.flexobject.hadoop.streaming.parquet.write.output;

import org.apache.parquet.io.DelegatingPositionOutputStream;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

import java.io.IOException;

public class ByteArrayOutputFile implements OutputFile {
    PositionByteArrayOutputStream outputStream;

    public ByteArrayOutputFile() {
    }

    private PositionOutputStream makeStream(){
        return new DelegatingPositionOutputStream(outputStream) {
            @Override
            public long getPos() throws IOException {
                return outputStream.getPos();
            }
        };
    }

    public byte[] toByteArray(){
        return outputStream.toByteArray();
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) throws IOException {
        outputStream = new PositionByteArrayOutputStream(blockSizeHint);
        return makeStream();
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
        if (outputStream == null)
            return create(blockSizeHint);
        return makeStream();
    }

    @Override
    public boolean supportsBlockSize() {
        return true;
    }

    @Override
    public long defaultBlockSize() {
        return 1024*1024;
    }
}
