package org.sv.flexobject.hadoop.mapreduce.input.parquet;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.hadoop.streaming.parquet.ParquetSchemaConf;
import org.sv.flexobject.hadoop.streaming.parquet.read.streamable.ParquetReadSupport;

import java.io.IOException;

public class StreamableParquetInputFormat extends ParquetInputFormat<StreamableWithSchema> {
    static Logger logger = Logger.getLogger(StreamableParquetInputFormat.class);

    public StreamableParquetInputFormat() {
        super(ParquetReadSupport.class);
    }

    @Override
    public RecordReader<Void, StreamableWithSchema> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        ParquetSchemaConf parquetConf = new ParquetSchemaConf().from(taskAttemptContext.getConfiguration());
        if (parquetConf.hasFilterPredicate()) {
            FilterPredicate predicate = parquetConf.getFilterPredicate();
            logger.info("Using Parquet predicate filter:" + predicate.toString());
            ParquetInputFormat.setFilterPredicate(taskAttemptContext.getConfiguration(), predicate);
        }

        return super.createRecordReader(inputSplit, taskAttemptContext);
    }
}
