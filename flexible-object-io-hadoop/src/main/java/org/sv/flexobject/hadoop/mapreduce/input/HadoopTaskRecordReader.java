package org.sv.flexobject.hadoop.mapreduce.input;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.sv.flexobject.hadoop.HadoopTask;
import org.sv.flexobject.stream.report.ProgressReporter;
import org.sv.flexobject.stream.report.SimpleProgressReporter;
import org.sv.flexobject.util.InstanceFactory;

import java.io.IOException;

public abstract class HadoopTaskRecordReader<KT,VT> extends RecordReader<KT,VT> {
    static Logger logger = Logger.getLogger(HadoopTaskRecordReader.class);

    private InputSplit split;
    private ProgressReporter progressReporter = InstanceFactory.get(SimpleProgressReporter.class);

    public void setProgressReporter(ProgressReporter progressReporter) {
        this.progressReporter = progressReporter;
    }

    public ProgressReporter getProgressReporter(){
        if (progressReporter == null)
            progressReporter = InstanceFactory.get(SimpleProgressReporter.class);
        return progressReporter;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return getProgressReporter().getProgress();
    }

    public InputSplit getSplit() {
        return split;
    }

    abstract protected void setupInput(InputSplit split, TaskAttemptContext context) throws IOException;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        try {
            HadoopTask.configure(context.getConfiguration());
        } catch (Exception e) {
            throw HadoopTask.getTaskConf().runtimeException(logger, "Failed to initialize HadoopTask", e);
        }
        this.split = split;
        setupInput(split, context);
    }

    protected abstract KT convertCurrentKey() throws Exception;

    @Override
    public KT getCurrentKey() throws IOException, InterruptedException {
        try {
            return convertCurrentKey();
        } catch (Exception e) {
            logger.error("Failed to convert current key", e);
            return null;
        }
    }

    protected abstract VT convertCurrentValue() throws Exception;

    @Override
    public VT getCurrentValue() throws IOException, InterruptedException {
        try {
            return convertCurrentValue();
        } catch (Exception e) {
            logger.error("Failed to convert current value", e);
            return null;
        }
    }
}
