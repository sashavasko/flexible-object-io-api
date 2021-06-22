package org.sv.flexobject.hadoop.mapreduce.input.key;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.sv.flexobject.hadoop.mapreduce.input.ConfiguredInputFormat;
import org.sv.flexobject.hadoop.mapreduce.input.InputConf;
import org.sv.flexobject.util.InstanceFactory;

import java.io.IOException;
import java.util.List;

public class KeyInputFormat<KT,VT> extends ConfiguredInputFormat<KT, VT> {

    @Override
    protected InputConf makeInputConf() {
        return InstanceFactory.get(KeyInputConf.class);
    }

    public static class LongLong extends KeyInputFormat<LongWritable,LongWritable> {}
    public static class LongText extends KeyInputFormat<LongWritable, Text> {}
    public static class LongDouble extends KeyInputFormat<LongWritable,DoubleWritable> {}
    public static class TextLong extends KeyInputFormat<Text,LongWritable> {}
    public static class TextText extends KeyInputFormat<Text, Text> {}
    public static class TextDouble extends KeyInputFormat<Text,DoubleWritable> {}
}
