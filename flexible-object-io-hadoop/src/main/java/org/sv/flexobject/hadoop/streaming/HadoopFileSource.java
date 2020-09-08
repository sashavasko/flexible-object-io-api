package org.sv.flexobject.hadoop.streaming;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.sv.flexobject.stream.sources.ByteStreamSource;
import org.sv.flexobject.util.ByteRepresentable;

import java.io.IOException;

public class HadoopFileSource<T extends ByteRepresentable> extends ByteStreamSource<T> {
    public HadoopFileSource(Configuration conf, Path path, Class<? extends ByteRepresentable> clazz) throws IOException {
        super(path.getFileSystem(conf).open(path), clazz);
    }
}
