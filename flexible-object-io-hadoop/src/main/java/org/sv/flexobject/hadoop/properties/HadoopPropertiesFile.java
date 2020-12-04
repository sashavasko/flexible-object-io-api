package org.sv.flexobject.hadoop.properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.sv.flexobject.properties.PropertiesFile;

import java.io.IOException;
import java.io.InputStream;

public class HadoopPropertiesFile extends PropertiesFile {

    Path impl;
    FileSystem fs;

    public HadoopPropertiesFile(String path, String name, Configuration conf) throws IOException {
        impl = new Path(path, name);
        fs = impl.getFileSystem(conf);
    }

    @Override
    public boolean exists() throws IOException {
        return fs.exists(impl);
    }

    @Override
    public boolean isDirectory() throws IOException {
        return fs.isDirectory(impl);
    }

    @Override
    public String getPath() {
        return impl.toString();
    }

    @Override
    public InputStream open() throws IOException {
        return fs.open(impl);
    }
}
