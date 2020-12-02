package org.sv.flexobject.mongo.adapter;

import com.fasterxml.jackson.databind.JsonNode;
import com.mongodb.DBObject;
import org.bson.Document;
import org.sv.flexobject.adapter.GenericOutAdapter;
import org.sv.flexobject.stream.Sink;

import java.sql.Date;
import java.sql.Timestamp;

public class MongoOutAdapter extends GenericOutAdapter<Document> {

    public MongoOutAdapter() {
    }

    public MongoOutAdapter(Sink<DBObject> sink) {
        super(sink);
    }

    @Override
    public Document createRecord() {
        return new Document();
    }

    protected void setValue(String fieldName, Object value){
        getCurrent().put(fieldName, value);
    }

    @Override
    public void setString(String paramName, String value) throws Exception {
        setValue(paramName, value);
    }

    @Override
    public void setJson(String paramName, JsonNode value) throws Exception {
        setValue(paramName, value);
    }

    @Override
    public void setInt(String paramName, Integer value) throws Exception {
        setValue(paramName, value);
    }

    @Override
    public void setBoolean(String paramName, Boolean value) throws Exception {
        setValue(paramName, value);
    }

    @Override
    public void setLong(String paramName, Long value) throws Exception {
        setValue(paramName, value);
    }

    @Override
    public void setDouble(String paramName, Double value) throws Exception {
        setValue(paramName, value);
    }

    @Override
    public void setDate(String paramName, Date value) throws Exception {
        setValue(paramName, value);
    }

    @Override
    public void setTimestamp(String paramName, Timestamp value) throws Exception {
        setValue(paramName, value);
    }
}
