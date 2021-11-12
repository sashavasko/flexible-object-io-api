package org.sv.flexobject.hadoop.mapreduce.input.key;

import org.sv.flexobject.hadoop.mapreduce.input.InputConf;
import org.sv.flexobject.properties.Namespace;

public final class KeyInputConf extends InputConf<KeyInputConf> {
    public static final String SUBNAMESPACE = "key";

    public KeyInputConf() {
        super(SUBNAMESPACE);
    }

    @Override
    protected String getSubNamespace() {
        return SUBNAMESPACE;
    }

    @Override
    public KeyInputConf setDefaults() {
        splitterClass = ModSplitter.class;
        readerClass = KeyRecordReader.LongText.class;
        return this;
    }

    public KeyInputConf(Namespace parent) {
        super(parent, SUBNAMESPACE);
    }
}
