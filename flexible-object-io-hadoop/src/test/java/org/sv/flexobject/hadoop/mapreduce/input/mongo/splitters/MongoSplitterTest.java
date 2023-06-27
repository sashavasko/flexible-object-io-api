package org.sv.flexobject.hadoop.mapreduce.input.mongo.splitters;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.CountOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.sv.flexobject.hadoop.mapreduce.input.mongo.MongoInputConf;
import org.sv.flexobject.hadoop.mapreduce.input.mongo.MongoSplit;
import org.sv.flexobject.hadoop.mapreduce.input.split.ProxyInputSplit;
import org.sv.flexobject.mongo.connection.MongoConnection;
import org.sv.flexobject.util.InstanceFactory;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class MongoSplitterTest {

    @Mock
    MongoInputConf conf;

    @Mock
    Configuration rawConf;

    @Mock
    MongoConnection connection;

    @Mock
    MongoCollection collection;

    @Mock
    CountOptions countOptions;

    @Mock
    MongoSplit split1;

    @Mock
    MongoSplit split2;

    MongoSplitter splitter;

    @Before
    public void setUp() throws Exception {
        InstanceFactory.set(MongoInputConf.class, conf);
        InstanceFactory.set(CountOptions.class, countOptions);
        doReturn(connection).when(conf).getMongo();
        doReturn(1).when(conf).getEstimateSizeLimit();
        doReturn(1).when(conf).getEstimateTimeLimitMicros();
        doReturn("foo").when(conf).getCollectionName();
        doReturn(collection).when(connection).getCollection("foo");

        splitter = Mockito.mock(MongoSplitter.class, Mockito.CALLS_REAL_METHODS);
    }

    @After
    public void tearDown() throws Exception {
        InstanceFactory.reset();
    }

    @Test
    public void setConf() {
        splitter.setConf(null);
        assertNull(splitter.getConf());
        splitter.setInputConf(conf);
        assertSame(conf, splitter.getInputConf());

        splitter.setConf(rawConf);

        assertSame(rawConf, splitter.getConf());
        verify(conf).from(rawConf);
    }

    @Test
    public void filterEmptySplits() throws Exception {

        doReturn(1l).when(split1).getLength(collection, 1, 1);
        doReturn(0l).when(split2).getLength(collection, 1, 1);
        splitter.setInputConf(conf);

        List<InputSplit> filtered = splitter.filterEmptySplits(Arrays.asList(new ProxyInputSplit(split1), new ProxyInputSplit(split2)));

        assertEquals(1, filtered.size());
        assertSame(split1, ((ProxyInputSplit)filtered.get(0)).getData());

        doReturn(1l).when(split1).getLength(collection, 1, 1);
        doReturn(1l).when(split2).getLength(collection, 1, 1);

        filtered = splitter.filterEmptySplits(Arrays.asList(new ProxyInputSplit(split1), new ProxyInputSplit(split2)));

        assertEquals(2, filtered.size());
        assertSame(split1, ((ProxyInputSplit)filtered.get(0)).getData());
        assertSame(split2, ((ProxyInputSplit)filtered.get(1)).getData());

        doReturn(0l).when(split1).getLength(collection, 1, 1);
        doReturn(0l).when(split2).getLength(collection, 1, 1);

        filtered = splitter.filterEmptySplits(Arrays.asList(new ProxyInputSplit(split1), new ProxyInputSplit(split2)));

        assertTrue(filtered.isEmpty());

    }
}