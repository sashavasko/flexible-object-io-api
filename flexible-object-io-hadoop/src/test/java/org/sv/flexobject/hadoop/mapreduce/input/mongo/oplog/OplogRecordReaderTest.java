package org.sv.flexobject.hadoop.mapreduce.input.mongo.oplog;

import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.BsonTimestamp;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.sv.flexobject.hadoop.mapreduce.input.split.ProxyInputSplit;
import org.sv.flexobject.stream.Source;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;

@RunWith(MockitoJUnitRunner.class)
public class OplogRecordReaderTest {


    OplogInputConf conf = new OplogInputConf();

    @Mock
    ProxyInputSplit proxyInputSplit;

    @Mock
    OplogSplit split;

    @Mock
    TaskAttemptContext context;

    @Mock
    Source source;

    OplogRecordReader reader;

    @Before
    public void setUp() throws Exception {
        reader = Mockito.mock(OplogRecordReader.class, Mockito.CALLS_REAL_METHODS);
        reader.setInputConf(conf);
        doReturn(source).when(reader).createSource(proxyInputSplit, context);
        doReturn(split).when(proxyInputSplit).getData();
    }

    @Test
    public void setupInput() throws Exception {
        conf.set("maxSecondsToExtract", 100);
        BsonTimestamp expectedTimestamp = new BsonTimestamp((int)(System.currentTimeMillis()/1000) - 100, 0);
        BsonTimestamp olderTimestamp = new BsonTimestamp((int)(System.currentTimeMillis()/1000) - 1000, 0);

        doReturn(olderTimestamp).when(reader).loadTimestamp();
        reader.setupInput(proxyInputSplit, context);

        assertEquals(expectedTimestamp, reader.lastTimestamp);
    }
}