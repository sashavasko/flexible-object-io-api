package org.sv.flexobject.hadoop.mapreduce.input.split;

import org.apache.hadoop.fs.Path;
import org.sv.flexobject.hadoop.properties.HadoopPropertiesWrapper;

public class PersistedInputSplitterConf extends HadoopPropertiesWrapper<PersistedInputSplitterConf> {
    public static final String SUBNAMESPACE = "input.splits";

    public String path;

    public Path getPath(){
        return new Path(path);
    }
}
