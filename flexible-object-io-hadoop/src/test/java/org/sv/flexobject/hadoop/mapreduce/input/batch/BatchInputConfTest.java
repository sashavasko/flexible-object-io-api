package org.sv.flexobject.hadoop.mapreduce.input.batch;

import org.junit.Test;
import org.sv.flexobject.schema.Schema;
import org.sv.flexobject.schema.SchemaElement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class BatchInputConfTest {

    @Test
    public void listSettings() {
        BatchInputConf conf = new BatchInputConf();
        List<String> expectedSettings = Arrays.asList(
                "org.sv.flexobject.input.batch.splitter.class",
                "org.sv.flexobject.input.batch.reader.class",
                "org.sv.flexobject.input.batch.source.builder.class",
                "org.sv.flexobject.input.batch.key.start",
                "org.sv.flexobject.input.batch.size",
                "org.sv.flexobject.input.batch.batches.per.split",
                "org.sv.flexobject.input.batch.batches.num",
                "org.sv.flexobject.input.batch.split.class",
                "org.sv.flexobject.input.batch.key.max.dataset.path",
                "org.sv.flexobject.input.batch.key.max.dataset.column.name",
                "org.sv.flexobject.input.batch.key.max.calculator",
                "org.sv.flexobject.input.batch.key.manager",
                "org.sv.flexobject.input.batch.key.column.name",
                "org.sv.flexobject.input.batch.reduce.max.keys");
        List<String> actualSettings = new ArrayList<>();

        for (SchemaElement e : Schema.getRegisteredSchema(conf.getClass()).getFields()){
            actualSettings.add(conf.getSettingName(e.getDescriptor().getName()));
        }

        assertEquals(expectedSettings, actualSettings);
    }

}