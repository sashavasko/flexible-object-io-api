package org.sv.flexobject.hadoop.mapreduce.input.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.sv.flexobject.hadoop.streaming.parquet.ParquetSchemaConf;
import org.sv.flexobject.util.InstanceFactory;

import java.io.IOException;

public class FilteredParquetInputFormat<T> extends ParquetInputFormat<T> {
    static protected Logger logger = Logger.getLogger(FilteredParquetInputFormat.class);

    Class <? extends ReadSupport<T>> readSupportClass;

    public <S extends ReadSupport<T>> FilteredParquetInputFormat(Class<S> readSupportClass) {
        super(readSupportClass);
        this.readSupportClass = readSupportClass;
    }

    protected Class<? extends ReadSupport<T>> getReadSupportClass() {
        return readSupportClass;
    }

    protected void checkConfiguredFilter(Configuration configuration){
        ParquetSchemaConf parquetConf = InstanceFactory.get(ParquetSchemaConf.class);
        parquetConf.from(configuration);
        if (parquetConf.hasFilterPredicate()) {
            FilterPredicate predicate = parquetConf.getFilterPredicate();
            logger.info("Using Parquet predicate filter:" + predicate.toString());
            ParquetInputFormat.setFilterPredicate(configuration, predicate);
        }
    }

    @Override
    public RecordReader<Void, T> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        checkConfiguredFilter(taskAttemptContext.getConfiguration());
        return super.createRecordReader(inputSplit, taskAttemptContext);
    }
}
