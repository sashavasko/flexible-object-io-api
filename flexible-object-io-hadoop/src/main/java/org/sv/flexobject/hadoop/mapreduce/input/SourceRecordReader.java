package org.sv.flexobject.hadoop.mapreduce.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.sv.flexobject.stream.Source;
import org.sv.flexobject.util.InstanceFactory;

import java.io.IOException;

public abstract class SourceRecordReader<K,V,SPLIT extends InputSplit> extends HadoopTaskRecordReader<K,V,SPLIT> implements InputConfOwner {
    protected Source<V> source;

    V currentValue;
    InputConf conf = null;
    Configuration rawConf;

    @Override
    public void setInputConf(InputConf conf) {
        this.conf = conf;
    }

    @Override
    public <T extends InputConf> T getInputConf() {
        return (T)conf.getClass().cast(conf);
    }

    public Configuration getConf() {
        return rawConf;
    }

    protected Source<V> createSource(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException{
        rawConf = taskAttemptContext.getConfiguration();
        if (conf == null) {
            conf = InstanceFactory.get(InputConf.class);
            conf.from(rawConf);
        }

        try {
            return conf.getSourceBuilder().build(inputSplit, taskAttemptContext);
        } catch (IllegalAccessException | InstantiationException e) {
            throw new IOException(conf.addDiagnostics("Failed to build source"), e);
        }
    }

    public Source<V> getSource() {
        return source;
    }

    @Override
    protected void setupInput(SPLIT split, TaskAttemptContext context) throws IOException {
        source = createSource(split, context);
        getProgressReporter().setSize(source);
    }

    protected boolean isValidRecord(V record){
        return record != null;
    }

    protected boolean hasMoreRecords(){
        return !source.isEOF();
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        if (hasMoreRecords()) {
            try {
                currentValue = source.get();
                getProgressReporter().increment();
                return isValidRecord(currentValue);
            } catch (Exception e) {
                throw new IOException("Failed to get next Value", e);
            }
        }
        return false;
    }

    protected abstract K extractKeyFromValue(V value);

    @Override
    protected K convertCurrentKey() {
        return extractKeyFromValue(currentValue);
    }

    @Override
    protected V convertCurrentValue() {
        return currentValue;
    }

    @Override
    public void close() throws IOException {
        try {
            source.close();
        } catch (Exception e) {
            if (e instanceof IOException)
                throw (IOException) e;
            throw new IOException("Failed to close source", e);
        }
    }
}
