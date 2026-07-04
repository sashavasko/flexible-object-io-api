package org.sv.flexobject.hadoop.mapreduce.input.key;

import org.junit.jupiter.api.Test;
import org.sv.flexobject.properties.Namespace;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

public class KeyInputConfTest {

    @Test
    public void listSettings() {
        KeyInputConf conf = new KeyInputConf();
        List<String> expectedSettings = Arrays.asList(
                "sv.input.key.splitter.class",
                "sv.input.key.reader.class",
                "sv.input.key.source.builder.class");
        List<String> actualSettings = conf.listSettings();

        assertEquals(expectedSettings, actualSettings);
    }

    @Test
    public void setDefaults() {
        KeyInputConf conf = new KeyInputConf();

        conf.setDefaults();

        assertSame(ModSplitter.class, conf.getSplitterClass());
        assertSame(KeyRecordReader.LongText.class, conf.getReaderClass());
    }

    @Test
    public void customNameSpace() {
        KeyInputConf conf = new KeyInputConf(new Namespace("foobar", "."));

        assertEquals("foobar.key", conf.getNamespace().toString());
    }
}