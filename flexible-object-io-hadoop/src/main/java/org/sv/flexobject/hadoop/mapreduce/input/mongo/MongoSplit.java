package org.sv.flexobject.hadoop.mapreduce.input.mongo;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.CountOptions;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.sv.flexobject.util.InstanceFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class MongoSplit extends InputSplit implements Writable {

    protected String queryJson = "";
    private Bson query;
    protected String projectionJson = "";
    private Bson projection;
    protected String sortJson = "";
    private Bson sort;
    protected Integer limit;
    protected Integer skip;
    protected Boolean noTimeout = false;
    protected Long estimatedLength = 1l;

    public MongoSplit() {
    }

    public MongoSplit(String queryJson, String projectionJson, String sortJson, Integer limit, Integer skip, Boolean noTimeout) {
        this.queryJson = queryJson;
        this.projectionJson = projectionJson;
        this.sortJson = sortJson;
        this.limit = limit;
        this.skip = skip;
        this.noTimeout = noTimeout;
    }

    public MongoSplit(String queryJson) {
        this.queryJson = queryJson;
    }

    public Bson getQuery(){
        if (query == null && hasQuery())
            query = Document.parse(queryJson);
        return query;
    }

    public long getLength(MongoCollection collection, int limit){
        return getLength(collection, limit, 0);
    }

    public long getLength(MongoCollection collection, int limit, int maxTimeMicros){
        CountOptions countOptions = InstanceFactory.get(CountOptions.class);

        if (hasLimit() && getLimit() < limit)
            countOptions.limit(getLimit());
        else
            countOptions.limit(limit);

        if (maxTimeMicros > 0)
            countOptions.maxTime(maxTimeMicros, TimeUnit.MICROSECONDS);

        if (hasSkip())
            countOptions.skip(skip);

        Bson query = getQuery();
        return collection.countDocuments(query, countOptions);
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return estimatedLength;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[0];
    }

    public void setEstimatedLength(Long estimatedLength) {
        this.estimatedLength = estimatedLength;
    }

    public boolean hasQuery() {
        return StringUtils.isNotBlank(queryJson);
    }

    public boolean hasProjection() {
        return StringUtils.isNotBlank(projectionJson);
    }

    public boolean hasSort() {
        return StringUtils.isNotBlank(sortJson);
    }

    public boolean hasLimit() {
        return limit != null && limit > 0;
    }

    public Bson getSort() {
        if (sort == null && hasSort())
            sort = Document.parse(sortJson);
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
            projection = Document.parse(projectionJson);
        return projection;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(queryJson);
        dataOutput.writeUTF(projectionJson);
        dataOutput.writeUTF(sortJson);
        dataOutput.writeInt(limit);
        dataOutput.writeInt(skip);
        dataOutput.writeBoolean(noTimeout);
        dataOutput.writeLong(estimatedLength);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        queryJson = dataInput.readUTF();
        projectionJson = dataInput.readUTF();
        sortJson = dataInput.readUTF();
        limit = dataInput.readInt();
        skip = dataInput.readInt();
        noTimeout = dataInput.readBoolean();
        estimatedLength = dataInput.readLong();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MongoSplit that = (MongoSplit) o;
        return queryJson.equals(that.queryJson) && projectionJson.equals(that.projectionJson) && sortJson.equals(that.sortJson) && Objects.equals(limit, that.limit) && Objects.equals(skip, that.skip) && Objects.equals(noTimeout, that.noTimeout) && estimatedLength.equals(that.estimatedLength);
    }

    @Override
    public int hashCode() {
        return queryJson.hashCode();
    }
}
