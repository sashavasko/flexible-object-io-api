package org.sv.flexobject.hadoop.mapreduce.input.mongo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.mongodb.MongoExecutionTimeoutException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.CountOptions;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.sv.flexobject.hadoop.StreamableAndWritableWithSchema;
import org.sv.flexobject.hadoop.mapreduce.input.split.InputSplitImpl;
import org.sv.flexobject.json.MapperFactory;
import org.sv.flexobject.mongo.json.BsonObjectToJsonConverter;
import org.sv.flexobject.schema.annotations.NonStreamableField;
import org.sv.flexobject.util.InstanceFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class MongoSplit extends StreamableAndWritableWithSchema implements InputSplitImpl {
    public static final Logger logger = Logger.getLogger(MongoSplit.class);

    protected JsonNode queryJson;
    @NonStreamableField
    private Bson query;

    protected JsonNode projectionJson;
    @NonStreamableField
    private Bson projection;

    protected JsonNode sortJson;
    @NonStreamableField
    private Bson sort;

    protected Integer limit;
    protected Integer skip;
    protected Boolean noTimeout = false;
    protected Long estimatedLength = 1l;

    public static Bson json2bson(JsonNode json){
        if (json != null){
            return Document.parse(json.toString());
        }
        return null;
    }

    public static JsonNode bson2json(Bson bson){
        if (bson != null) {
            try {
//                return BsonObjectToJsonConverter.getInstance().convert(bson); // This does not handle ObjectIds properly TODO!!!
                return MapperFactory.getObjectReader().readTree(bson.toBsonDocument().toJson());
            } catch (IOException e) {
                throw new RuntimeException("Failed to convert bson to json :" + bson.toBsonDocument().toString(), e);
            }
        }
        return null;
    }

    public static class Builder{
        private Bson query;
        private Bson projection;
        private Bson sort;
        protected Integer limit;
        protected Integer skip;
        protected Boolean noTimeout = false;

        public Builder query(Bson query){
            this.query = query;
            return this;
        }

        public Builder projection(Bson projection){
            this.projection = projection;
            return this;
        }

        public Builder sort(Bson sort){
            this.sort = sort;
            return this;
        }

        public Builder limit(int limit){
            this.limit = limit;
            return this;
        }

        public Builder skip(int skip){
            this.skip = skip;
            return this;
        }

        public Builder noTimeout(){
            this.noTimeout = true;
            return this;
        }

        public MongoSplit build(MongoCollection collection, int estimateLimit, int maxTimeMicros){
            MongoSplit split = InstanceFactory.get(MongoSplit.class);
            split.query = query;
            split.queryJson = bson2json(query);

            split.projection = projection;
            split.projectionJson = bson2json(projection);

            split.sort = sort;
            split.sortJson = bson2json(sort);

            split.limit = limit;
            split.skip = skip;
            split.noTimeout = noTimeout;

            long estimatedLength = split.getLength(collection, estimateLimit, maxTimeMicros);
            split.setEstimatedLength(estimatedLength);
            return split;
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public MongoSplit() {
    }

    public MongoSplit(String queryJson, String projectionJson, String sortJson, Integer limit, Integer skip, Boolean noTimeout) throws JsonProcessingException {
        this.queryJson = MapperFactory.getObjectReader().readTree(queryJson);
        this.projectionJson = MapperFactory.getObjectReader().readTree(projectionJson);
        this.sortJson = MapperFactory.getObjectReader().readTree(sortJson);
        this.limit = limit;
        this.skip = skip;
        this.noTimeout = noTimeout;
    }

    public MongoSplit(String queryJson) throws JsonProcessingException {
        this.queryJson = MapperFactory.getObjectReader().readTree(queryJson);
    }

    public MongoSplit(JsonNode queryJson) {
        this.queryJson = queryJson;
    }

    public Bson getQuery(){
        if (query == null && hasQuery())
            query = json2bson(queryJson);
        return query;
    }

    public long getLength(MongoCollection collection, int limit){
        return getLength(collection, limit, 0);
    }

    public long getLength(MongoCollection collection, int limit, int maxTimeMicros){
        CountOptions countOptions = InstanceFactory.get(CountOptions.class);

        int actualLimit = (hasLimit() && getLimit() < limit) ? getLimit() : limit;
        countOptions.limit(actualLimit);

        if (maxTimeMicros > 0)
            countOptions.maxTime(maxTimeMicros, TimeUnit.MICROSECONDS);

        if (hasSkip())
            countOptions.skip(skip);

        Bson query = getQuery();
        try {
            return collection.countDocuments(query, countOptions);
        } catch(MongoExecutionTimeoutException e){
            logger.error("Exceeded timeout to count records in a split. Assuming Limit of " + actualLimit);
            return actualLimit;
        }
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return estimatedLength == null ? 0 : estimatedLength;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[0];
    }

    public void setEstimatedLength(Long estimatedLength) {
        this.estimatedLength = estimatedLength;
    }

    public boolean hasQuery() {
        return query != null || queryJson != null;
    }

    public boolean hasProjection() {
        return projection != null || projectionJson != null;
    }

    public boolean hasSort() {
        return sort != null || sortJson != null;
    }

    public boolean hasLimit() {
        return limit != null && limit > 0;
    }

    public Bson getSort() {
        if (sort == null && hasSort())
            sort = json2bson(sortJson);
        return sort;
    }

    public Integer getLimit() {
        return limit;
    }

    public Integer getSkip() {
        return skip;
    }

    public Boolean isNotimeout() {
        return noTimeout;
    }

    public boolean hasSkip() {
        return skip != null && skip > 0;
    }

    public Bson getProjection() {
        if (projection == null && hasProjection())
            projection = json2bson(projectionJson);
        return projection;
    }

    @Override
    public int hashCode() {
        return queryJson.hashCode();
    }
}
