package org.sv.flexobject.properties;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/*
 * Wrapper for the standard Java File class, so that we can extend FilePropertiesProvider for Hadoop etc.
 */
public class PropertiesFile {

    File impl;

    public PropertiesFile() {
    }

    public PropertiesFile(String path, String name){
        impl = new File(path, name);
    }


    public boolean exists() throws IOException {
        return impl.exists();
    }

    public boolean isDirectory() throws IOException {
        return impl.isDirectory();
    }

    public String getPath() {
        return impl.getPath();
    }

    public InputStream open() throws IOException {
        return new FileInputStream(impl);
    }
}
