package org.sv.flexobject.mongo.adapter;

import com.fasterxml.jackson.databind.JsonNode;
import com.mongodb.DBObject;
import org.bson.Document;
import org.sv.flexobject.adapter.GenericInAdapter;
import org.sv.flexobject.json.MapperFactory;
import org.sv.flexobject.mongo.json.BsonObjectToJsonConverter;
import org.sv.flexobject.stream.Source;
import org.sv.flexobject.stream.sources.SingleValueSource;
import org.sv.flexobject.util.ConsumerWithException;
import org.sv.flexobject.util.InstanceFactory;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.Map;

public class MongoInAdapter extends GenericInAdapter<Map<String,Object>> {

    BsonObjectToJsonConverter converter = InstanceFactory.get(BsonObjectToJsonConverter.class);

    public MongoInAdapter() {
    }

    public MongoInAdapter(Source source) {
        super(source);
    }

    @Override
    public String getString(String fieldName) throws Exception {
        return (String) getCurrent().get(fieldName);
    }

    @Override
    public JsonNode getJson(String fieldName) throws Exception {
        Object data = getCurrent().get(fieldName);
        if (data instanceof Document || data instanceof DBObject){
            return converter.convert(data);
        }
        if (data instanceof JsonNode) {
            return (JsonNode) data;
        }else if (data instanceof String){
            return MapperFactory.getObjectReader().readTree((String)data);
        }else
            return null;
    }

    @Override
    public Integer getInt(String fieldName) throws Exception {
        return (Integer) getCurrent().get(fieldName);
    }

    @Override
    public Boolean getBoolean(String fieldName) throws Exception {
        return (Boolean) getCurrent().get(fieldName);
    }

    @Override
    public Long getLong(String fieldName) throws Exception {
        return (Long) getCurrent().get(fieldName);
    }

    @Override
    public Double getDouble(String fieldName) throws Exception {
        return (Double) getCurrent().get(fieldName);
    }

    @Override
    public Date getDate(String fieldName) throws Exception {
        Object dateO = getCurrent().get(fieldName);
        if (dateO == null)
            return null;
        if (dateO instanceof LocalDate)
            return Date.valueOf((LocalDate) dateO);
        return (Date) dateO;
    }

    @Override
    public Timestamp getTimestamp(String fieldName) throws Exception {
        return (Timestamp) getCurrent().get(fieldName);
    }

    public static void consume(Document bson, ConsumerWithException<MongoInAdapter, Exception> consumer) throws Exception {
        if (bson != null) {
            SingleValueSource<Document> source = new SingleValueSource<>(bson);
            MongoInAdapter adapter = new MongoInAdapter(source);
            adapter.next();

            consumer.accept(adapter);
        }
    }


}
