package org.sv.flexobject.hadoop.streaming.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.schema.MessageType;
import org.joda.time.DateTime;
import org.joda.time.MutableDateTime;
import org.junit.After;
import org.junit.Test;
import org.sv.flexobject.hadoop.streaming.parquet.json.ParquetJsonSink;
import org.sv.flexobject.json.MapperFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ParquetUtilsTest {

    Path testFilepath = new Path("test.parquet");
    Path testDataPath = new Path("testData/");;


    @After
    public void tearDown() throws Exception {
        Configuration conf = new Configuration();
        testDataPath.getFileSystem(conf).delete(testDataPath, true);
        testFilepath.getFileSystem(conf).delete(testFilepath, false);
    }

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
    public void getMaxLongValueInFile() throws Exception {
        Configuration conf = new Configuration();
        writeTestFile(conf, testFilepath, 1234567890l, 98765432);

        assertEquals(98765432, ParquetUtils.getMaxValueInFile(conf, testFilepath, "int32Field"));
        assertEquals(1234567890l, ParquetUtils.getMaxValueInFile(conf, testFilepath, "int64Field"));
        testFilepath.getFileSystem(conf).delete(testFilepath, false);

    }

    @Test
    public void getMaxLongValueInEmptyFile() throws Exception {
        Configuration conf = new Configuration();
        writeEmptyFile(conf, testFilepath);

        assertNull(ParquetUtils.getMaxValueInFile(conf, testFilepath, "int64Field"));
        assertNull(ParquetUtils.getMaxValueInFile(conf, testFilepath, "int32Field"));
        testFilepath.getFileSystem(conf).delete(testFilepath, false);

    }

    private static void writeTestFile(Configuration conf, Path testFilepath, long int64Field, int int32Field) throws Exception {
        MessageType schema = new MessageType("test",
                ParquetSchema.integerField("int32Field"),
                ParquetSchema.longField("int64Field"));

        try (ParquetJsonSink sink = ParquetJsonSink.builder()
                .forOutput(testFilepath)
                .withSchema(schema)
                .withConf(conf).build()) {
            sink.put(MapperFactory.getObjectReader().readTree("{\"int32Field\":" + int32Field + ",\"int64Field\":" + int64Field + "}"));
        }
    }

    private static void writeEmptyFile(Configuration conf, Path testFilepath) throws Exception {
        try (ParquetJsonSink sink = ParquetJsonSink.builder()
                .forOutput(testFilepath)
                .withSchema(new MessageType("test",
                        ParquetSchema.integerField("int32Field"),
                        ParquetSchema.longField("int64Field")))
                .withConf(conf).build()) {
        }
    }

    @Test
    public void getMaxLongValueInFiles() throws Exception {
        Configuration conf = new Configuration();
        Path testFilepath = new Path(testDataPath, "test1.parquet");
        Path test2Filepath = new Path(testDataPath, "test2.parquet");
        Path wildcardPath = testDataPath;
        writeTestFile(conf, testFilepath, 111l, 1111);
        writeTestFile(conf, test2Filepath, 222l, 2222);

        assertEquals(222l, ParquetUtils.getMaxValueInFiles(conf, wildcardPath, true, "int64Field"));
        assertEquals(2222, ParquetUtils.getMaxValueInFiles(conf, wildcardPath, true, "int32Field"));
        testFilepath.getFileSystem(conf).delete(testFilepath, false);
        testFilepath.getFileSystem(conf).delete(test2Filepath, false);
    }
}