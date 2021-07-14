package org.sv.flexobject.hadoop.mapreduce.input;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.sv.flexobject.hadoop.mapreduce.input.DaoRecordReaderConf;
import org.sv.flexobject.hadoop.mapreduce.util.MRDao;
import org.sv.flexobject.schema.Schema;
import org.sv.flexobject.schema.SchemaElement;
import org.sv.flexobject.util.InstanceFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;
import static org.junit.Assert.assertSame;

@RunWith(MockitoJUnitRunner.class)
public class DaoRecordReaderConfTest {

    @Mock
    MRDao mockMRDao;

    DaoRecordReaderConf conf = new DaoRecordReaderConf();
    Configuration rawConf = new Configuration(false);

    @After
    public void tearDown() throws Exception {
        InstanceFactory.reset();
    }

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

    @Test
    public void customNamespace() {
        DaoRecordReaderConf conf = new DaoRecordReaderConf("foo.bar");

        assertEquals("foo.bar.record.reader", conf.getNamespace());
    }

    @Test
    public void setDefault() {
        assertEquals(DaoRecordReader.CURRENT_KEY_FIELD_NAME, conf.getKeyFieldName());
        assertEquals(DaoRecordReader.CURRENT_VALUE_FIELD_NAME, conf.getValueFieldName());
        assertEquals(DaoRecordReader.DEFAULT_MAX_RETRIES_VALUE, conf.getMaxRetries());
    }

    @Test
    public void isDaoConfigured() {
        assertFalse(conf.isDaoConfigured());

        rawConf.set("org.sv.flexobject.record.reader.dao.class", String.class.getName());

        conf.from(rawConf);
        assertTrue(conf.isDaoConfigured());
    }

    @Test
    public void createDao() throws InstantiationException, IllegalAccessException {
        rawConf.set("org.sv.flexobject.record.reader.dao.class", String.class.getName());
        conf.from(rawConf);
        InstanceFactory.set(String.class, mockMRDao);

        assertSame(mockMRDao, conf.createDao());
    }
}