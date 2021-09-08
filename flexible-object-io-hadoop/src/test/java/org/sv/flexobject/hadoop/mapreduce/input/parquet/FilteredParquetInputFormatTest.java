package org.sv.flexobject.hadoop.mapreduce.input.parquet;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.sv.flexobject.hadoop.streaming.parquet.ParquetFilterParser;
import org.sv.flexobject.hadoop.streaming.parquet.ParquetSchema;
import org.sv.flexobject.hadoop.streaming.parquet.ParquetSchemaConf;
import org.sv.flexobject.json.MapperFactory;
import org.sv.flexobject.properties.Namespace;
import org.sv.flexobject.testdata.TestDataWithSubSchema;
import org.sv.flexobject.util.InstanceFactory;

import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.intColumn;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class FilteredParquetInputFormatTest {

    ParquetSchemaConf parquetConf;

    Configuration rawConf;

    FilterPredicate predicate = eq(intColumn("columnName"), 1);

    FilteredParquetInputFormat format;

    @Before
    public void setUp() throws Exception {
        parquetConf = new ParquetSchemaConf(new Namespace("test", "."));
        rawConf = new Configuration(false);

        InstanceFactory.set(ParquetSchemaConf.class, parquetConf);

        format = Mockito.mock(FilteredParquetInputFormat.class, Mockito.CALLS_REAL_METHODS);

    }

    @After
    public void tearDown() throws Exception {
        InstanceFactory.reset();
    }

    @Test
    public void createRecordReader() throws JsonProcessingException {
        rawConf.set("test.parquet.input.schema.class", TestDataWithSubSchema.class.getName());
        String predicateJson = "{'eq':{'string':'foo','value':'bar'}}".replace("'", "\"");
        rawConf.set("test.parquet.filter.predicate.json", predicateJson);
        format.checkConfiguredFilter(rawConf);

        assertEquals(ParquetSchema.forClass(TestDataWithSubSchema.class), parquetConf.getInputSchema());
        assertTrue(parquetConf.hasFilterPredicate());
        assertEquals(ParquetFilterParser.json2FilterPredicate(MapperFactory.getObjectReader().readTree(predicateJson)), parquetConf.getFilterPredicate());
    }

    @Test
    public void createRecordReaderWithFilter() {
        String predicateJson = "{'eq':{'int':'columnName','value':1}}".replace("'", "\"");
        rawConf.set("test.parquet.filter.predicate.json", predicateJson);

        format.checkConfiguredFilter(rawConf);

        assertEquals(predicate.toString(), rawConf.get("parquet.private.read.filter.predicate.human.readable"));
    }
}