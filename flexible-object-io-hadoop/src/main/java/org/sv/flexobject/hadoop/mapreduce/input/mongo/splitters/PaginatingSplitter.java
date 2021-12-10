package org.sv.flexobject.hadoop.mapreduce.input.mongo.splitters;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.sv.flexobject.hadoop.mapreduce.input.mongo.MongoSplit;
import org.sv.flexobject.hadoop.mapreduce.input.split.ProxyInputSplit;
import org.sv.flexobject.mongo.streaming.MongoBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PaginatingSplitter extends MongoSplitter {

    @Override
    public List<InputSplit> split(Configuration rawConf) throws IOException {
        PaginatedInputConf conf = getInputConf();
        List<InputSplit> splits = new ArrayList<>();
        try(MongoBuilder mongoBuilder = conf.getMongoBuilder()) {
            MongoCollection collection;

            try {
                collection = mongoBuilder.getCollection();
                collection.find().limit(1);
            } catch (Exception e) {
                throw new IOException(conf.addDiagnostics("Failed to get mongo collection"), e);
            }

            Bson keyProjection = conf.makeSplitProjection();
            Object minBound = null;
            Object maxBound = null;

            do {
                FindIterable<Document> iterable;
                if (minBound == null) {
                    iterable = collection.find();
                } else {
                    iterable = collection.find(conf.makeQuery(minBound, null));
                }

                try (MongoCursor<Document> cursor = iterable.projection(keyProjection)
                        .sort(conf.makeSort())
                        .skip(conf.minDocs)
                        .limit(1)
                        .noCursorTimeout(true).cursor()) {

                    if (cursor.hasNext()) {
                        maxBound = cursor.next().get(conf.getSplitKey());
                    } else {
                        maxBound = null;
                    }
                }

                MongoSplit split = MongoSplit.builder()
                        .query(conf.makeQuery(minBound, maxBound))
                        .sort(conf.makeSort())
                        .db(conf.getDbName())
                        .collection(conf.getCollectionName())
                        .noTimeout()
                        .length(conf.getMinDocs())
                        .build();

                splits.add(new ProxyInputSplit(split));

                minBound = maxBound;

            } while (maxBound != null);
        } catch (Exception e) {
            if (e instanceof IOException)
                throw (IOException) e;
            else
                throw conf.runtimeException("Failed to connect to Mongo DB", e);
        }
        return splits;
    }
}
