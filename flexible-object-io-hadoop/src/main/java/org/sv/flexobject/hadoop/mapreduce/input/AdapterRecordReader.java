package org.sv.flexobject.hadoop.mapreduce.input;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import org.sv.flexobject.InAdapter;

import java.io.IOException;

public abstract class AdapterRecordReader<KT,VT> extends HadoopTaskRecordReader<KT,VT> {
    protected static Logger logger = Logger.getLogger(AdapterRecordReader.class);

    private InAdapter input;

    public InAdapter getInput() {
        return input;
    }

    public class LongField {
        LongWritable v = new LongWritable();

        public LongWritable convert(String fieldName) throws Exception {
            v.set(input.getLong(fieldName));
            return v;
        }
    }

    public LongField longField(){
        return new LongField();
    }

    public class TextField {
        Text v = new Text();

        public Text convert(String fieldName) throws Exception {
            v.set(input.getString(fieldName));
            return v;
        }
    }

    public TextField textField(){
        return new TextField();
    }

    abstract protected InAdapter createAdapter(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException;

    public void setInput(InAdapter input) {
        if (this.input != null) {
            try {
                this.input.close();
            } catch (Exception e) {
                logger.error("Failed to close previous Input Adapter", e);
            }
        }
        this.input = input;
        getProgressReporter().setSize(input);
    }

    @Override
    public void close() throws IOException {
        try {
            if (input != null)
                input.close();
        } catch (Exception e) {
            logger.error("Failed to close reader", e);
        }
    }
}
