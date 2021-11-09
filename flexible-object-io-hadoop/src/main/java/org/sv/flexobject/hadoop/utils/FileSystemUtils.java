package org.sv.flexobject.hadoop.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.FileNotFoundException;
import java.io.IOException;

public class FileSystemUtils {

    public static Long readLongFromFile(Configuration conf, Path path) throws IOException {
        FileSystem fs = path.getFileSystem(conf);
        FileStatus fileStatus;
        try {
            fileStatus = fs.getFileStatus(path);
        } catch (FileNotFoundException e){
            return null;
        }
        if (fileStatus == null || !fileStatus.isFile())
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


    public static FileStatus getMostRecentFolder(Configuration conf, Path path) throws IOException {
        FileSystem fs = path.getFileSystem(conf);
        RemoteIterator<FileStatus> iterator = fs.listStatusIterator(path);
        FileStatus mostRecent = null;
        while(iterator.hasNext()){
            FileStatus item = iterator.next();
            if(item.isDirectory()) {
                if (mostRecent == null || mostRecent.getModificationTime() <= item.getModificationTime())
                    mostRecent = item;
            }
        }
        return mostRecent;
    }

}
