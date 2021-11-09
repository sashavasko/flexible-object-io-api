package org.sv.flexobject.hadoop.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.ClusterMapReduceTestCase;
import org.junit.Test;

import static org.junit.Assert.*;

public class FileSystemUtilsTest extends ClusterMapReduceTestCase {

    @Test
    public void writeLongToFile() throws Exception {
        startCluster(true, null);
        Configuration conf = createJobConf();
        FileSystem fs = getFileSystem();
        Path target = new Path ("/longValue");

        assertNull(FileSystemUtils.readLongFromFile(conf, target));

        long value = 123456789l;
        FileSystemUtils.writeLongToFile(conf, target, value);

        long actualValue = FileSystemUtils.readLongFromFile(conf, target);

        assertEquals(value, actualValue);
    }

    @Test
    public void getMostRecentFolder() throws Exception {
        startCluster(true, null);
        Configuration conf = createJobConf();
        FileSystem fs = getFileSystem();

        Path parent = new Path ("/parent");
        Path childOld = new Path (parent, "old");
        Path childNew = new Path (parent, "new");
        assertTrue(fs.mkdirs(parent));
        assertTrue(fs.mkdirs(childOld));
        Thread.sleep(2000);
        assertTrue(fs.mkdirs(childNew));

        FileStatus status = fs.getFileStatus(parent);
        assertTrue(status.isDirectory());
        status = fs.getFileStatus(childNew);
        assertTrue(status.isDirectory());

//        fs.listFiles(parent, false);

        FileStatus mostRecent = FileSystemUtils.getMostRecentFolder(conf, parent);

        assertEquals(fs.makeQualified(childNew), mostRecent.getPath());
    }
}