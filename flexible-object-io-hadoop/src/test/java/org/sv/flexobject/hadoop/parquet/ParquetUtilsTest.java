package org.sv.flexobject.hadoop.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.schema.MessageType;
import org.joda.time.DateTime;
import org.joda.time.MutableDateTime;
import org.junit.Test;
import org.sv.flexobject.hadoop.streaming.parquet.streamable.ParquetSink;
import org.sv.flexobject.testdata.levelone.ObjectWithNestedObject;

import static org.junit.Assert.*;

public class ParquetUtilsTest {

    @Test
    public void testTranDateAsNumberOfDaysSinceEpoch() throws Exception {
        MutableDateTime vd = new MutableDateTime(2016, 1, 1, 0, 0, 0, 0);//"2016-01-01");

// Prior to Apache Drill 1.9:        assertEquals(4897977, ParquetUtils.jodaDateToParquetDate(vd));
        assertEquals(16801, ParquetUtils.jodaDateToParquetDate(vd));

        assertEquals(vd, ParquetUtils.parquetDateToMutableDateTime(4897977));
        assertEquals(vd, ParquetUtils.parquetDateToMutableDateTime(16801));

    }

    @Test
    public void testTranDateAsNumberOfDaysSinceEpochDT() throws Exception {
        DateTime dt = new DateTime(2016, 1, 1, 0, 0, 0, 0);//"2016-01-01");

        assertEquals(dt, ParquetUtils.parquetDateToDateTime(4897977));
        assertEquals(dt, ParquetUtils.parquetDateToDateTime(16801));
    }

    @Test
    public void getMaxLongValueInFileWithComplexStructure() throws Exception {
        Configuration conf = new Configuration();
        Path testFilepath = new Path("testData/test.parquet");
        Path wildcardPath = new Path("testData/");

        try {
            testFilepath.getFileSystem(conf).delete(wildcardPath, true);
        }catch (Exception dontcare){}

        ObjectWithNestedObject o1 = ObjectWithNestedObject.random();
        ObjectWithNestedObject o2 = ObjectWithNestedObject.random();
        try (ParquetSink sink = ParquetSink.builder()
                .forOutput(testFilepath)
                .withSchema(ObjectWithNestedObject.class)
                .withConf(conf).build()) {
            sink.put(o1);
            sink.put(o2);
        }

        int expectedValue = Math.max(o1.nestedObject.intField, o2.nestedObject.intField);
        assertEquals(expectedValue, ParquetUtils.getMaxValueInFiles(conf, wildcardPath, true, "nestedObject.intField"));
        testFilepath.getFileSystem(conf).delete(wildcardPath, true);

    }

}