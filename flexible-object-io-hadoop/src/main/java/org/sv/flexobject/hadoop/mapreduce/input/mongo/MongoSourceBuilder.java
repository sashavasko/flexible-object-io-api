package org.sv.flexobject.hadoop.mapreduce.input.mongo;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.sv.flexobject.hadoop.mapreduce.input.InputConf;
import org.sv.flexobject.hadoop.mapreduce.input.InputConfOwner;
import org.sv.flexobject.hadoop.mapreduce.input.SourceBuilder;
import org.sv.flexobject.hadoop.mapreduce.input.split.ProxyInputSplit;
import org.sv.flexobject.mongo.streaming.MongoBuilder;
import org.sv.flexobject.mongo.streaming.MongoSource;
import org.sv.flexobject.stream.Source;
import org.sv.flexobject.util.InstanceFactory;

import java.io.IOException;

public class MongoSourceBuilder implements SourceBuilder, InputConfOwner {

    MongoInputConf conf;

    @Override
    public Source build(InputSplit split, TaskAttemptContext context) throws InstantiationException, IllegalAccessException, IOException {
        if (conf == null) {
            conf = InstanceFactory.get(MongoInputConf.class);
            conf.from(context.getConfiguration());
        }

        MongoBuilder builder = conf.getMongoBuilder();
        MongoSplit mongoSplit = ((ProxyInputSplit) split).getData();
        if (mongoSplit.hasQuery())
            builder.filter(mongoSplit.getQuery());
        if (mongoSplit.hasLimit())
            builder.limit(mongoSplit.getLimit());
        if (mongoSplit.hasSkip())
            builder.skip(mongoSplit.getSkip());
        if (mongoSplit.hasProjection())
            builder.projection(mongoSplit.getProjection());
        if (mongoSplit.hasSort())
            builder.sort(mongoSplit.getSort());
        if (mongoSplit.isNotimeout())
            builder.noTimeout();

        if (conf.hasSchema())
            builder.schema(conf.getInputSchema());

        try {
            return builder.build();
        } catch (Exception e) {
            throw new IOException(conf.addDiagnostics("Failed to query Mongo"), e);
        }
    }

    @Override
    public void setInputConf(InputConf conf) {
        this.conf = (MongoInputConf) conf;
    }

    @Override
    public InputConf getInputConf() {
        return conf;
    }
}
