package org.sv.flexobject.hadoop.mapreduce.input.mongo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.bson.conversions.Bson;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.sv.flexobject.hadoop.mapreduce.input.split.ProxyInputSplit;
import org.sv.flexobject.mongo.streaming.MongoSource;
import org.sv.flexobject.testdata.TestDataWithSubSchema;
import org.sv.flexobject.util.InstanceFactory;

import java.io.IOException;

import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class MongoSourceBuilderTest {
    @Mock
    MongoSplit split;

    @Mock
    TaskAttemptContext context;

    @Mock
    Configuration rawConf;

    @Mock
    MongoInputConf conf;

    @Mock
    MongoSource.Builder mongoBuilder;

    @Mock
    Bson query;

    @Mock
    Bson projection;

    @Mock
    Bson sort;

    @Mock
    MongoSource result;

    MongoSourceBuilder builder = new MongoSourceBuilder();

    @Before
    public void setUp() throws Exception {
        InstanceFactory.set(MongoInputConf.class, conf);
        InstanceFactory.set(MongoSource.Builder.class, mongoBuilder);

        doReturn(rawConf).when(context).getConfiguration();
        doReturn(result).when(mongoBuilder).build();
        doReturn(mongoBuilder).when(conf).getMongoBuilder();
    }

    @After
    public void tearDown() throws Exception {
        InstanceFactory.reset();
    }

    @Test
    public void build() throws Exception {
        doReturn(query).when(split).getQuery();
        doReturn(true).when(split).hasQuery();
        doReturn(10).when(split).getLimit();
        doReturn(true).when(split).hasLimit();
        doReturn(5).when(split).getSkip();
        doReturn(true).when(split).hasSkip();
        doReturn(projection).when(split).getProjection();
        doReturn(true).when(split).hasProjection();
        doReturn(sort).when(split).getSort();
        doReturn(true).when(split).hasSort();
        doReturn(true).when(split).isNotimeout();

        doReturn(true).when(conf).hasSchema();

        assertSame(result, builder.build(new ProxyInputSplit(split), context));

        verify(conf).from(rawConf);
        verify(mongoBuilder).filter(query);
        verify(mongoBuilder).limit(10);
        verify(mongoBuilder).skip(5);
        verify(mongoBuilder).projection(projection);
        verify(mongoBuilder).sort(sort);
        verify(mongoBuilder).noTimeout();
        verify(mongoBuilder).build();
    }

    @Test
    public void buildWithNoValues() throws Exception {
        doReturn(query).when(split).getQuery();
        doReturn(false).when(split).hasQuery();
        doReturn(false).when(split).hasLimit();
        doReturn(false).when(split).hasSkip();
        doReturn(false).when(split).hasProjection();
        doReturn(false).when(split).hasSort();
        doReturn(false).when(split).isNotimeout();

        doReturn(false).when(conf).hasSchema();

        assertSame(result, builder.build(new ProxyInputSplit(split), context));

        verify(conf).from(rawConf);
        verify(mongoBuilder).build();
        verifyNoMoreInteractions(mongoBuilder);
    }

    @Test
    public void buildWithException() throws Exception {
        RuntimeException toThrow = new RuntimeException("foobar");
        doThrow(toThrow).when(mongoBuilder).build();

        try{
            builder.build(new ProxyInputSplit(split), context);
            throw new RuntimeException("should have thrown");
        }catch (IOException e){
            assertSame(toThrow, e.getCause());
        }
    }
}