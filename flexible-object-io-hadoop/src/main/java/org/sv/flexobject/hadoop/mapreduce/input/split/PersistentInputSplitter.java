package org.sv.flexobject.hadoop.mapreduce.input.split;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.InputSplit;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.hadoop.mapreduce.input.Splitter;
import org.sv.flexobject.hadoop.streaming.parquet.streamable.ParquetSource;
import org.sv.flexobject.util.InstanceFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PersistentInputSplitter extends Configured implements Splitter {
    @Override
    public List<InputSplit> split(Configuration rawConf) throws IOException {
        setConf(rawConf);
        PersistedInputSplitterConf conf = InstanceFactory.get(PersistedInputSplitterConf.class);
        conf.from(rawConf);

        List<InputSplit> splits = new ArrayList<>();

        Path sourcePath = conf.getPath();
        FileSystem fs = sourcePath.getFileSystem(rawConf);

        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(sourcePath, true);
        while(iterator.hasNext()){
            LocatedFileStatus fileStatus = iterator.next();
            if (fileStatus.isFile() && fileStatus.getPath().getName().endsWith(".parquet")){
                loadSplitsFromFile(fileStatus.getPath(), splits);
            }
        }

        return splits;
    }

    protected void loadSplitsFromFile(Path path, List<InputSplit> splits) throws IOException {
        try(ParquetSource source = ParquetSource.builder()
                .withConf(getConf())
                .forInput(path)
                .build()){
            while(!source.isEOF()){
                StreamableWithSchema data = source.get();
                splits.add(new ProxyInputSplit((InputSplitImpl) data));
            }
        } catch (Exception e) {
            throw new IOException("Failed to load splits from file " + path, e);
        }
    }
}
