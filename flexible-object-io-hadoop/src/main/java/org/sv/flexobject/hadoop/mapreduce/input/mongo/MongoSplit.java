package org.sv.flexobject.hadoop.mapreduce.input.mongo;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MongoSplit extends InputSplit implements Writable {

    protected String queryJson;
    protected String projectionJson;
    protected String sortJson;
    protected Integer limit;
    protected Integer skip;
    protected Boolean notimeout = false;

    public MongoSplit() {
    }

    public MongoSplit(String queryJson) {
        this.queryJson = queryJson;
    }

    public Bson getQuery(){
        return Document.parse(queryJson);
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[0];
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(queryJson);
        dataOutput.writeUTF(projectionJson);
        dataOutput.writeUTF(sortJson);
        dataOutput.writeInt(limit);
        dataOutput.writeInt(skip);
        dataOutput.writeBoolean(notimeout);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        queryJson = dataInput.readUTF();
        projectionJson = dataInput.readUTF();
        sortJson = dataInput.readUTF();
        limit = dataInput.readInt();
        skip = dataInput.readInt();
        notimeout = dataInput.readBoolean();
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
        return Document.parse(sortJson);
    }

    public Integer getLimit() {
        return limit;
    }

    public Integer getSkip() {
        return skip;
    }

    public Boolean isNotimeout() {
        return notimeout;
    }

    public boolean hasSkip() {
        return skip != null && skip > 0;
    }


    public Bson getProjection() {
        return Document.parse(projectionJson);
    }
}
