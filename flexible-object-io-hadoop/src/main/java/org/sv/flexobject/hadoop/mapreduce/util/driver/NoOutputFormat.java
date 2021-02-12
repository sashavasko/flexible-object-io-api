package org.sv.flexobject.hadoop.mapreduce.util.driver;

import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

public class NoOutputFormat extends OutputFormat {
    private static class NoOutputRecordWriter extends RecordWriter {
        @Override
        public void write(Object key, Object value) throws IOException, InterruptedException { /* do nothing  */ }
        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {/* do nothing  */ }
    }

    private static class NoOutputCommiter extends OutputCommitter {
        @Override
        public void setupJob(JobContext jobContext) throws IOException {}

        @Override
        public void setupTask(TaskAttemptContext taskContext) throws IOException {}

        @Override
        public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {return false;}

        @Override
        public void commitTask(TaskAttemptContext taskContext) throws IOException {}

        @Override
        public void abortTask(TaskAttemptContext taskContext) throws IOException {}
    }

    @Override
    public RecordWriter getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new NoOutputRecordWriter();
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {}

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new NoOutputCommiter();
    }
}
