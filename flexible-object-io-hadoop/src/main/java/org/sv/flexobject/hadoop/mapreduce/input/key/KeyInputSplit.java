package org.sv.flexobject.hadoop.mapreduce.input.key;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class KeyInputSplit<KT extends Writable> extends InputSplit implements Writable {

    protected KT key;

    public static class LongKeySplit extends KeyInputSplit<LongWritable> {
        public LongKeySplit() {
            key = new LongWritable();
        }

        public LongKeySplit(LongWritable key) {
            super(key);
        }
    }
    public static class TextKeySplit extends KeyInputSplit<Text> {
        public TextKeySplit() {
            key = new Text();
        }

        public TextKeySplit(Text key) {
            super(key);
        }
    }

    public KeyInputSplit(){}
    public KeyInputSplit(KT key){ this.key = key;}

    public KT getKey() {
        return key;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return 2;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[0];
    }

    @Override
    public void write(DataOutput out) throws IOException {
        key.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        key.readFields(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyInputSplit<?> that = (KeyInputSplit<?>) o;
        return Objects.equals(key, that.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key);
    }

    @Override
    public String toString() {
        return "LongKeySplit{" +
                "key=" + key.toString() +
                '}';
    }
}
