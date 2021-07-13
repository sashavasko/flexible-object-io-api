package org.sv.flexobject.hadoop.mapreduce.input;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.sv.flexobject.stream.Source;
import org.sv.flexobject.util.InstanceFactory;

import java.io.IOException;
import java.util.function.Function;

public abstract class SourceRecordReader<K,V> extends HadoopTaskRecordReader<K,V> {
    protected Source<V> source;
    protected Function<V,K> keyExtractor;

    V currentValue;

    protected Source<V> createSource(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException{
        InputConf conf = InstanceFactory.get(InputConf.class);
        conf.from(taskAttemptContext.getConfiguration());

        try {
            return conf.getSourceBuilder().build(inputSplit, taskAttemptContext);
        } catch (IllegalAccessException | InstantiationException e) {
            throw new IOException("Failed to build source", e);
        }
    }

    @Override
    protected void setupInput(InputSplit split, TaskAttemptContext context) throws IOException {
        source = createSource(split, context);
        getProgressReporter().setSize(source);
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        if (source.isEOF())
            return false;

        try {
            currentValue = source.get();
            getProgressReporter().increment();
            return true;
        } catch (Exception e) {
            throw new IOException("Failed to get next Value", e);
        }
    }

    @Override
    protected K convertCurrentKey() {
        return keyExtractor.apply(currentValue);
    }

    @Override
    protected V convertCurrentValue() {
        return currentValue;
    }

    @Override
    public void close() throws IOException {
        if (source instanceof AutoCloseable) {
            try {
                ((AutoCloseable) source).close();
            } catch (Exception e) {
                if (e instanceof IOException)
                    throw (IOException) e;
                throw new IOException("Failed to close source", e);
            }
        } else
            source.setEOF();
    }
}
