package org.sv.flexobject.hadoop.mapreduce.input.split;

import org.apache.hadoop.fs.Path;
import org.sv.flexobject.hadoop.properties.HadoopPropertiesWrapper;
import org.sv.flexobject.properties.Namespace;

public class PersistedInputSplitterConf extends HadoopPropertiesWrapper<PersistedInputSplitterConf> {
    public static final String SUBNAMESPACE = "input.splits";

    public String path;

    @Override
    protected String getSubNamespace() {
        return SUBNAMESPACE;
    }

    public PersistedInputSplitterConf() {
        super(SUBNAMESPACE);
    }

    public PersistedInputSplitterConf(Namespace parent) {
        super(parent, SUBNAMESPACE);
    }

    @Override
    public PersistedInputSplitterConf setDefaults() {
        path = "splits";
        return this;
    }

    public Path getPath(){
        return new Path(path);
    }
}
