package org.sv.flexobject.hadoop.mapreduce.input;

import org.apache.hadoop.mapreduce.RecordReader;
import org.sv.flexobject.hadoop.mapreduce.input.key.KeyRecordReader;
import org.sv.flexobject.hadoop.mapreduce.input.key.ModSplitter;
import org.sv.flexobject.hadoop.properties.HadoopPropertiesWrapper;
import org.sv.flexobject.properties.Namespace;
import org.sv.flexobject.util.InstanceFactory;

public class InputConf<SELF extends HadoopPropertiesWrapper> extends HadoopPropertiesWrapper<SELF> {
    public static final String SUBNAMESPACE = "input";

    protected Class<? extends Splitter> splitterClass;
    protected Class<? extends RecordReader> readerClass;
    protected Class<? extends SourceBuilder> sourceBuilderClass;

    public InputConf() {
        super(SUBNAMESPACE);
    }

    public InputConf(String child) {
        super(makeMyNamespace(getParentNamespace(InputConf.class), SUBNAMESPACE), child);
    }

    public InputConf(Namespace parent) {
        super(parent, SUBNAMESPACE);
    }

    public InputConf(Namespace parent, String child) {
        super(parent, child);
    }

    @Override
    public SELF setDefaults() {
        return (SELF)this;
    }

    public Class<? extends Splitter> getSplitterClass() {
        return splitterClass == null ? ModSplitter.class : splitterClass;
    }
    public Splitter getSplitter() {
        return InstanceFactory.get(getSplitterClass());
    }

    public Class<? extends RecordReader> getReaderClass() {
        return readerClass == null ? KeyRecordReader.LongText.class : readerClass;
    }

    public RecordReader getReader() {
        return InstanceFactory.get(getReaderClass());
    }

    public SourceBuilder getSourceBuilder() throws IllegalArgumentException {
        if (sourceBuilderClass == null)
            throw new IllegalArgumentException("Missing Source Builder class");
        return InstanceFactory.get(sourceBuilderClass);
    }

}
