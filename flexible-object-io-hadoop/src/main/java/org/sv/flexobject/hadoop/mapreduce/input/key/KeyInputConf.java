package org.sv.flexobject.hadoop.mapreduce.input.key;

import org.sv.flexobject.hadoop.properties.HadoopPropertiesWrapper;

public class KeyInputConf extends HadoopPropertiesWrapper<KeyInputConf> {
    public static final String SUBNAMESPACE = "input.key";

    public Class<? extends KeySplitter> splitterClass = ModSplitter.class;
    public Class<? extends KeyRecordReader> readerClass = KeyRecordReader.LongText.class;

    public KeyInputConf() {
    }

    public KeyInputConf(String namespace) {
        super(namespace);
    }

    @Override
    public String getSubNamespace() {
        return SUBNAMESPACE;
    }
}
