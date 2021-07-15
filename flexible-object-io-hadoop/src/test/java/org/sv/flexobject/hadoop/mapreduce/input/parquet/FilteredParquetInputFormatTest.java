package org.sv.flexobject.hadoop.mapreduce.input.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.sv.flexobject.hadoop.streaming.parquet.ParquetSchemaConf;
import org.sv.flexobject.util.InstanceFactory;

import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.intColumn;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class FilteredParquetInputFormatTest {

    @Mock
    ParquetSchemaConf parquetConf;
    @Mock
    Configuration rawConf;

    FilterPredicate predicate = eq(intColumn("columnName"), 1);

    FilteredParquetInputFormat format;

    @Before
    public void setUp() throws Exception {
        InstanceFactory.set(ParquetSchemaConf.class, parquetConf);

        format = Mockito.mock(FilteredParquetInputFormat.class, Mockito.CALLS_REAL_METHODS);

    }

    @After
    public void tearDown() throws Exception {
        InstanceFactory.reset();
    }

    @Test
    public void createRecordReader() {

        format.checkConfiguredFilter(rawConf);

        verify(parquetConf).from(rawConf);
        verify(parquetConf).hasFilterPredicate();
    }

    @Test
    public void createRecordReaderWithFilter() {
        doReturn(predicate).when(parquetConf).getFilterPredicate();
        doReturn(true).when(parquetConf).hasFilterPredicate();

        format.checkConfiguredFilter(rawConf);

        verify(parquetConf).from(rawConf);
        verify(parquetConf).hasFilterPredicate();

        verify(rawConf).set("parquet.private.read.filter.predicate.human.readable", predicate.toString());
    }
}