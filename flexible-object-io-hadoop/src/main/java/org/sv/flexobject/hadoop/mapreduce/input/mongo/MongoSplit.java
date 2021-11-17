package org.sv.flexobject.hadoop.mapreduce.input.mongo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoExecutionTimeoutException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.CountOptions;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.sv.flexobject.hadoop.StreamableAndWritableWithSchema;
import org.sv.flexobject.hadoop.mapreduce.input.split.InputSplitImpl;
import org.sv.flexobject.json.MapperFactory;
import org.sv.flexobject.mongo.json.BsonToJsonConverter;
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

    protected String hosts;
    protected String dbName;
    protected String collectionName;

    protected Long estimatedLength = 1l;

    @NonStreamableField
    protected static final BsonToJsonConverter bsonToJsonConverter = new BsonToJsonConverter();

    public static Bson json2bson(JsonNode json){
        if (json != null){
            return Document.parse(json.toString());
        }
        return null;
    }

    public static JsonNode bson2json(Bson bson){
        if (bson != null) {
            try {
                return bsonToJsonConverter.convert(bson);
            } catch (IOException e) {
                throw new RuntimeException("Failed to convert bson to json :" + bson.toBsonDocument().toString(), e);
            }
        }
        return null;
    }

    public static class Builder{
        private Class<? extends MongoSplit> splitClass = MongoSplit.class;
        private Bson query;
        private Bson projection;
        private Bson sort;
        protected Integer limit;
        protected Integer skip;
        protected Boolean noTimeout = false;
        protected String hosts;
        protected String dbName;
        protected String collectionName;
        protected Long estimatedLength;

        public Builder splitClass(Class<? extends MongoSplit> splitClass){
            this.splitClass = splitClass;
            return this;
        }

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

        public Builder length(long estimatedLength){
            this.estimatedLength = estimatedLength;
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

        public Builder hosts(String hosts){
            this.hosts = hosts;
            return this;
        }

        public Builder db(String dbName){
            this.dbName = dbName;
            return this;
        }

        public Builder collection(String collectionName){
            this.collectionName = collectionName;
            return this;
        }

        public Builder clone(MongoSplit other){
            splitClass = other.getClass();
            query = other.query;
            projection = other.projection;
            sort = other.sort;
            limit = other.limit;
            skip = other.skip;
            noTimeout = other.noTimeout;
            hosts = other.hosts;
            dbName = other.dbName;
            collectionName = other.collectionName;
            estimatedLength = other.estimatedLength;
            return this;
        }

        public <T extends MongoSplit> T build(){
            return build(null, 0, 0);
        }
        public <T extends MongoSplit> T  build(MongoCollection collection, int estimateLimit, int maxTimeMicros){
            MongoSplit split = InstanceFactory.get(splitClass);
            split.query = query;
            split.queryJson = bson2json(query);

            split.projection = projection;
            split.projectionJson = bson2json(projection);

            split.sort = sort;
            split.sortJson = bson2json(sort);

            split.limit = limit;
            split.skip = skip;
            split.noTimeout = noTimeout;

            split.hosts = hosts;
            split.dbName = dbName;
            split.collectionName = collectionName;

            if (estimatedLength == null) {
                int retries = 5;
                RuntimeException lastException = null;
                estimatedLength = -1l;
                while (estimatedLength < 0 && --retries >= 0) {
                    try {
                        estimatedLength = split.getLength(collection, estimateLimit, maxTimeMicros);
                    } catch (MongoCommandException e) {
                        lastException = e;
                    }
                }
                if (estimatedLength < 0 && lastException != null)
                    throw lastException;
            }
            split.setEstimatedLength(estimatedLength);
            return (T)split;
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public MongoSplit() {
    }

    public MongoSplit(JsonNode queryJson) {
        this.queryJson = queryJson;
    }

    public MongoSplit(String queryJson, String projectionJson, String sortJson, Integer limit, Integer skip, Boolean noTimeout) throws JsonProcessingException {
        this(MapperFactory.getObjectReader().readTree(queryJson));
        this.projectionJson = MapperFactory.getObjectReader().readTree(projectionJson);
        this.sortJson = MapperFactory.getObjectReader().readTree(sortJson);
        this.limit = limit;
        this.skip = skip;
        this.noTimeout = noTimeout;
    }

    public MongoSplit(String queryJson) throws JsonProcessingException {
        this(MapperFactory.getObjectReader().readTree(queryJson));
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

    public boolean hasHosts() {
        return StringUtils.isNotBlank(hosts);
    }

    public boolean hasDbName() {
        return StringUtils.isNotBlank(dbName);
    }

    public boolean hasCollectionName() {
        return StringUtils.isNotBlank(collectionName);
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

    public String getHosts() {
        return hosts;
    }

    public String getDbName() {
        return dbName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    @Override
    public int hashCode() {
        return queryJson.hashCode();
    }
}
