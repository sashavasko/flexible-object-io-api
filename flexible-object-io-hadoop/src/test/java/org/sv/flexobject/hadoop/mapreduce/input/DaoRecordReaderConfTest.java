package org.sv.flexobject.hadoop.mapreduce.input;

import org.junit.Test;
import org.sv.flexobject.hadoop.mapreduce.input.DaoRecordReaderConf;
import org.sv.flexobject.schema.Schema;
import org.sv.flexobject.schema.SchemaElement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class DaoRecordReaderConfTest {

    @Test
    public void listSettings() {
        DaoRecordReaderConf conf = new DaoRecordReaderConf();
        List<String> expectedSettings = Arrays.asList(
                "org.sv.flexobject.record.reader.key.field.name",
                "org.sv.flexobject.record.reader.value.field.name",
                "org.sv.flexobject.record.reader.max.retries",
                "org.sv.flexobject.record.reader.dao.class");
        List<String> actualSettings = new ArrayList<>();

        for (SchemaElement e : Schema.getRegisteredSchema(conf.getClass()).getFields()){
            actualSettings.add(conf.getSettingName(e.getDescriptor().getName()));
        }

        assertEquals(expectedSettings, actualSettings);
    }
}