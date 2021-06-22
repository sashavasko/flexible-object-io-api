package org.sv.flexobject.hadoop.mapreduce.input.key;

import org.sv.flexobject.hadoop.mapreduce.input.InputConf;

public class KeyInputConf extends InputConf<KeyInputConf> {
    public static final String SUBNAMESPACE = "input.key";

    public KeyInputConf() {
        super();
    }

    @Override
    public KeyInputConf setDefaults() {
        splitterClass = ModSplitter.class;
        readerClass = KeyRecordReader.LongText.class;
        return this;
    }

    public KeyInputConf(String namespace) {
        super(namespace);
    }

    @Override
    public String getSubNamespace() {
        return SUBNAMESPACE;
    }
}
