package org.sv.flexobject.hadoop.mapreduce.input.key;

import org.sv.flexobject.hadoop.properties.HadoopPropertiesWrapper;

public class KeyInputConf extends HadoopPropertiesWrapper<KeyInputConf> {
    public static final String SUBNAMESPACE = "input.key";

    private Class<? extends KeySplitter> splitterClass = ModSplitter.class;
    private Class<? extends KeyRecordReader> readerClass = KeyRecordReader.LongText.class;

    public KeyInputConf() {
    }

    public KeyInputConf(String namespace) {
        super(namespace);
    }

    @Override
    public String getSubNamespace() {
        return SUBNAMESPACE;
    }

    public KeySplitter getSplitter() throws IllegalAccessException, InstantiationException {
        return splitterClass == null ? new ModSplitter() : splitterClass.newInstance();
    }

    public KeyRecordReader getReader() throws IllegalAccessException, InstantiationException {
        return readerClass == null ? new KeyRecordReader.LongText() : readerClass.newInstance();
    }
}
