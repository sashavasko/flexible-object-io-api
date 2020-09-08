package org.sv.flexobject.hadoop.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class FileSystemUtils {

    public static Long readLongFromFile(Configuration conf, Path path) throws IOException {
        FileSystem fs = path.getFileSystem(conf);
        if (!fs.isFile(path))
            return null;
        try (FSDataInputStream is = fs.open(path)) {
            long l = 0;
            int b;
            while ((b = is.read()) > 0) {
                if (Character.isDigit(b)) {
                    l = l * 10 + b - '0';
                }
            }

            return l;
        }
    }

    public static void writeLongToFile(Configuration conf, Path path, long value) throws IOException {
        FileSystem fs = path.getFileSystem(conf);
        try (FSDataOutputStream os = fs.create(path, true)) {
            os.write(String.valueOf(value).getBytes());
        }
    }
}
