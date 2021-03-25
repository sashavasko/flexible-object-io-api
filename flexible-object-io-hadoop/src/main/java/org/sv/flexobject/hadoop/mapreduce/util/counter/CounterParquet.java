package org.sv.flexobject.hadoop.mapreduce.util.counter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.sv.flexobject.hadoop.HadoopTask;
import org.sv.flexobject.hadoop.streaming.parquet.write.streamable.ParquetWriterBuilder;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class CounterParquet extends Counter implements AutoCloseable {

    boolean passThrough = false;

    Map<String, Long> counters = new HashMap<>();

    public CounterParquet(String groupName) {
        super(groupName);
    }

    public CounterParquet(String groupName, boolean passThrough) {
        super(groupName);
        this.passThrough = passThrough;
    }

    @Override
    protected void increment(String name, int value) {
        Long countValue = counters.get(name);
        if(countValue == null){
            counters.put(name, (long) value);
        }else {
            counters.put(name, countValue + value);
        }

        if (passThrough)
            super.increment(name, value);
    }

    @Override
    public void close() throws Exception {
        if(!counters.isEmpty()){
            Configuration conf = context.getConfiguration();
            TaskAttemptID taskId = context.getTaskAttemptID();
            Timestamp endTimestamp = new Timestamp(System.currentTimeMillis());
            try(ParquetWriter fileWriter = ParquetWriterBuilder.forPath(makeCounterPath(conf, taskId))
                    .withSchema(CounterItem.class)
                    .withConf(conf)
                    .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                    .withCompressionCodec(CompressionCodecName.SNAPPY).build()){

                CounterItem item = new CounterItem(groupName, startTimestamp, endTimestamp, taskId.toString());

                for (Map.Entry<String, Long> counter : counters.entrySet()){
                    item.setCounterName(counter.getKey());
                    item.setCounterValue(counter.getValue());
                    fileWriter.write(item);
                }
            }
            counters.clear();
        }
    }

    protected Path makeCounterPath(Configuration conf, TaskAttemptID taskId) {
        // TODO change property name
        String counterLocation = conf.get(HadoopTask.getTaskConf().getNamespace() + ".parquet.counters");
        String fileName = counterLocation + "/counter_for_" + taskId + ".snappy.parquet";
        return new Path (fileName);
    }

    public long getCounter(String name){
        Long countValue = counters.get(name);
        return countValue == null ? 0l : countValue;
    }
}
