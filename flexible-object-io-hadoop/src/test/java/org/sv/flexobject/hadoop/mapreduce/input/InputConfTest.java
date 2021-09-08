package org.sv.flexobject.hadoop.mapreduce.input;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.sv.flexobject.hadoop.mapreduce.input.key.KeyRecordReader;
import org.sv.flexobject.hadoop.mapreduce.input.key.ModSplitter;
import org.sv.flexobject.properties.Namespace;
import org.sv.flexobject.util.InstanceFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

@RunWith(MockitoJUnitRunner.class)
public class InputConfTest {

    @Mock
    SourceBuilder mockBuilder;

    @Mock
    ModSplitter mockSplitter;

    @Mock
    KeyRecordReader.LongText mockReader;

    Configuration rawConf;
    InputConf conf;
    @Before
    public void setUp() throws Exception {
        rawConf = new Configuration();
        conf = new InputConf();
    }

    @After
    public void tearDown() throws Exception {
        InstanceFactory.reset();
    }

    @Test
    public void constructor() {
        conf = new InputConf(Namespace.forPath(".", "foo", "bar"));

        assertEquals("foo.bar.input", conf.getNamespace().toString());
    }

    @Test
    public void setDefaults() {
        assertSame(conf, conf.setDefaults());
    }

    @Test
    public void getSubNamespace() {
        assertEquals("sv.input", new InputConf<>().getNamespace().getNamespace());
    }

    @Test
    public void getSplitterClass() {
        assertSame(ModSplitter.class, conf.getSplitterClass());

        rawConf.set("sv.input.splitter.class", String.class.getName());
        conf.from(rawConf);
        assertSame(String.class, conf.getSplitterClass());
    }

    @Test
    public void getSplitter() {
        InstanceFactory.set(ModSplitter.class, mockSplitter);
        assertSame(mockSplitter, conf.getSplitter());

        InstanceFactory.set(String.class, mockSplitter);
        rawConf.set("sv.input.splitter.class", String.class.getName());
        conf.from(rawConf);
        assertSame(mockSplitter, conf.getSplitter());
    }

    @Test
    public void getReaderClass() {
        assertSame(KeyRecordReader.LongText.class, conf.getReaderClass());

        rawConf.set("sv.input.reader.class", String.class.getName());
        conf.from(rawConf);
        assertSame(String.class, conf.getReaderClass());
    }

    @Test
    public void getReader() {
        InstanceFactory.set(KeyRecordReader.LongText.class, mockReader);
        assertSame(mockReader, conf.getReader());

        InstanceFactory.set(String.class, mockReader);
        rawConf.set("sv.input.reader.class", String.class.getName());
        conf.from(rawConf);
        assertSame(mockReader, conf.getReader());
    }

    @Test
    public void getSourceBuilder() throws InstantiationException, IllegalAccessException {
        try {
            conf.getSourceBuilder();
            throw new RuntimeException("should have thrown");
        }catch (IllegalArgumentException e){
            assertEquals("Missing Source Builder class", e.getMessage());
        }
        rawConf.set("sv.input.source.builder.class", String.class.getName());
        InstanceFactory.set(String.class, mockBuilder);
        conf.from(rawConf);

        assertSame(mockBuilder, conf.getSourceBuilder());
    }
}