package org.sv.flexobject.hadoop.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;

import javax.activation.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class HDFSFileDataSource extends Configured implements DataSource {
    private Path path;

    public HDFSFileDataSource(Path path, Configuration conf) {
        this.path = path;
        setConf(conf);
    }

    public HDFSFileDataSource(String name, Configuration conf) {
        this(new Path(name), conf);
    }

    public InputStream getInputStream() throws IOException {
        return path.getFileSystem(getConf()).open(path);
    }

    public OutputStream getOutputStream() throws IOException {
        return path
                .getFileSystem(getConf())
                .create(path);
    }

    public String getContentType() {
        return "text/plain";
    }

    public String getName() {
        return this.path.getName();
    }
}
