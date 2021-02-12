package org.sv.flexobject.hadoop.mapreduce.output.parquet;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.parquet.hadoop.ParquetOutputCommitter;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.sv.flexobject.StreamableWithSchema;
import org.sv.flexobject.hadoop.streaming.parquet.write.streamable.ParquetWriteSupport;

import java.io.IOException;

public class ParquetNoEmptyOutputFormat<V> extends ParquetOutputFormat<V> {
    protected FileOutputCommitter committer;
    protected boolean outputWritten = false;

    public <S extends WriteSupport<V>> ParquetNoEmptyOutputFormat(S writeSupport) {
        super(writeSupport);
    }

    @Override
    public RecordWriter<Void, V> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        final RecordWriter<Void, V> recWriter = super.getRecordWriter(taskAttemptContext);

        // wrap writer to track that something was output
        return new RecordWriter<Void, V>() {
            @Override
            public void write(Void key, V value) throws IOException,
                    InterruptedException {
                outputWritten = true;
                recWriter.write(key, value);
            }

            @Override
            public void close(TaskAttemptContext context) throws IOException,
                    InterruptedException {
                recWriter.close(context);
            }
        };
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException {
        if (committer == null){
            Path output = getOutputPath(context);
            committer = new ParquetOutputCommitter(output, context) {
                @Override
                public boolean needsTaskCommit(TaskAttemptContext context)
                        throws IOException {
                    return outputWritten && super.needsTaskCommit(context);
                }
            };
        }
        return committer;
    }
}
