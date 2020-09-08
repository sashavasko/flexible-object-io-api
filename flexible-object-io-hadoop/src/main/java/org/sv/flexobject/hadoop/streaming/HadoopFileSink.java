package org.sv.flexobject.hadoop.streaming;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.sv.flexobject.stream.sinks.ByteStreamSink;
import org.sv.flexobject.util.ByteRepresentable;

import java.io.IOException;

public class HadoopFileSink<T extends ByteRepresentable> extends ByteStreamSink<T> {
    public HadoopFileSink(Configuration conf, Path path, boolean overwrite) throws IOException {
        super(path.getFileSystem(conf).create(path, overwrite));
    }
}
