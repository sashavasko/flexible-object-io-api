package org.sv.flexobject.hadoop.mapreduce.util.cacheable;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.log4j.Logger;
import org.sv.flexobject.hadoop.mapreduce.util.counter.Counter;
import org.sv.flexobject.hadoop.mapreduce.util.driver.OutputProducer;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

public abstract class HadoopCacheable extends Configured {

    static Logger logger = Logger.getLogger(HadoopCacheable.class);

    protected Path locationOnHDFS;
    protected String counterTitle;
    protected String tag;

    protected Counter counter = new Counter(this.getClass().getName());

    public HadoopCacheable(Path locationOnHDFS, String counterTitle, String tag){
        this.locationOnHDFS = locationOnHDFS;
        this.counterTitle = counterTitle;
        this.tag = tag;
    }

    public abstract HadoopCacheable add(String combination);
    public abstract void clear();

    public void countRecord(String counterSubTitle){
        counter.increment(counterSubTitle);
    }

    public static void cacheFiles(OutputProducer driver, Path locationOnHDFS, String tag) throws IOException, URISyntaxException {
        FileSystem hdfs = driver.getHDFS();
        RemoteIterator<LocatedFileStatus> files = hdfs.listFiles(locationOnHDFS, true);
        LocatedFileStatus file;
        int count = 0;
        while (files.hasNext() && (file = files.next())!= null){
            if (file.getLen() > 0) {
                Path filePath = file.getPath();
                driver.getJob().addCacheFile(new URI(filePath.toString() + "#" + tag + count));
                count++;
            }
        }

        logger.info("Added " + count + " files to cache from " + locationOnHDFS.toString());
    }

    public void cacheFiles(OutputProducer driver) throws IOException, URISyntaxException {
        cacheFiles(driver, locationOnHDFS, tag);
    }

    public void loadData (BufferedReader reader) throws IOException {
        String combination;
        while ((combination = reader.readLine()) != null) {
            add (combination);
            counter.increment("items loaded");
        }
    }
    public FileInputStream openFile(String filename) throws FileNotFoundException {
        return new FileInputStream(filename);
    }

    public void loadCachedFile(String filename, String path) throws IOException {
        FileInputStream fis = openFile(filename);
        if (fis != null) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
            loadData(reader);
            reader.close();
            counter.increment("files loaded");
            fis.close();
        }
    }

    public void load(TaskInputOutputContext context) throws IOException {
        setConf(context.getConfiguration());
        counter.setContext(context).setMainTitle(counterTitle);

        URI[] uris = context.getCacheFiles(); // getCacheFiles returns null

        if (uris == null)
            return;

        for (URI uri : uris) {
            String fragment = uri.getFragment();

            if(fragment.startsWith(tag)) {
                String uriString = uri.toString();
                loadCachedFile(fragment, uriString.substring(0, uriString.length()-1-fragment.length()));
            }
        }
    }

}
