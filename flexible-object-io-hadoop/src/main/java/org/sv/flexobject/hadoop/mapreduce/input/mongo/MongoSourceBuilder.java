package org.sv.flexobject.hadoop.mapreduce.input.mongo;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.sv.flexobject.hadoop.mapreduce.input.SourceBuilder;
import org.sv.flexobject.mongo.streaming.MongoSource;
import org.sv.flexobject.stream.Source;

import java.io.IOException;

public class MongoSourceBuilder implements SourceBuilder {

    @Override
    public Source build(InputSplit split, TaskAttemptContext context) throws InstantiationException, IllegalAccessException, IOException {
        MongoInputConf conf = new MongoInputConf();
        conf.from(context.getConfiguration());

        MongoSource.Builder builder = MongoSource.builder()
                .connection(conf.getConnectionName())
                .db(conf.getDbName())
                .collection(conf.getCollectionName());
        MongoSplit mongoSplit = (MongoSplit) split;
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
}
