package org.sv.flexobject.hadoop.mapreduce.input.key;

import org.junit.Test;
import org.sv.flexobject.schema.Schema;
import org.sv.flexobject.schema.SchemaElement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class KeyInputConfTest {

    @Test
    public void listSettings() {
        KeyInputConf conf = new KeyInputConf();
        List<String> expectedSettings = Arrays.asList("org.sv.flexobject.hadoop.input.key.splitter.class", "org.sv.flexobject.hadoop.input.key.reader.class");
        List<String> actualSettings = new ArrayList<>();

        for (SchemaElement e : Schema.getRegisteredSchema(conf.getClass()).getFields()){
            actualSettings.add(conf.getSettingName(e.getDescriptor().getName()));
        }

        assertEquals(expectedSettings, actualSettings);
    }


}